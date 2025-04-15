import random
import uuid
from datetime import UTC, datetime, timezone
from decimal import Decimal

from geh_common.data_products.measurements_core.measurements_gold import current_v1
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

orchestration_instance_id = uuid.uuid4()
job_parameters = {"orchestration-instance-id": orchestration_instance_id}

# TODO XHTCA: brug funktionen fra __init__
# def create_metering_point_id(position=8, digit=9) -> str:
#     id = "".join(random.choice("0123456789") for _ in range(18))
#     return id[:position] + str(digit) + id[position + 1 :]


def create_metering_point_id():
    return "999999999999999999"


def test__calculated_measurements_v1__is_streamable(spark: SparkSession) -> None:
    # Arrange
    config = EnvironmentConfiguration()
    metering_point_id = create_metering_point_id()
    quantity = Decimal(f"{random.uniform(1.0, 100.0):.3f}")

    base_job_fixture = JobTestFixture(
        environment_configuration=config,
        job_name="NetConsumptionGroup6",
        job_parameters=job_parameters,
    )

    database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
    table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME

    statement_seed = f"""
      INSERT INTO {config.catalog_name}.{database_name}.{table_name} VALUES
      (
        '{"electrical_heating"}',
        '{orchestration_instance_id}',
        '{metering_point_id}',
        '{uuid.uuid4()}',
        '{datetime.now(UTC)}',
        '{"electrical_heating"}',
        '{datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)}',
        {quantity}
      )
    """
    response_seed = base_job_fixture.execute_statement(statement_seed)
    assert response_seed.result.row_count == 1

    # Act
    statement = f"""
                  SELECT *
                  FROM {config.catalog_name}.{current_v1.database_name}.{current_v1.view_name}
                  WHERE metering_point_id = '{metering_point_id}' and quantity = {quantity}
                  LIMIT 1
                """
    base_job_fixture.wait_for_data(statement)
