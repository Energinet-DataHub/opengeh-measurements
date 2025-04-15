import random
import uuid
from datetime import UTC, datetime, timezone
from decimal import Decimal

from geh_common.data_products.measurements_core.measurements_gold import current_v1
from geh_common.databricks.databricks_api_client import DatabricksApiClient
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

orchestration_instance_id = uuid.uuid4()
job_parameters = {"orchestration-instance-id": orchestration_instance_id}


def create_quantity(min=0.00, max=999999999999999.999) -> Decimal:
    return Decimal(random.uniform(min, max)).quantize(Decimal("1.000"))


def test__calculated_measurements_v1__is_streamable(spark: SparkSession) -> None:
    # Arrange
    config = EnvironmentConfiguration()

    quantity = create_quantity()

    databricks_api_client = DatabricksApiClient(config.databricks_token, config.workspace_url)
    # base_job_fixture = JobTestFixture(
    #     environment_configuration=config,
    #     job_name="",
    #     job_parameters=job_parameters,
    # )

    database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
    table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME

    statement_seed = f"""
      INSERT INTO {config.catalog_name}.{database_name}.{table_name} VALUES
      (
        '{"electrical_heating"}',
        '{orchestration_instance_id}',
        '{170000030000000201}',
        '{uuid.uuid4()}',
        '{datetime.now(UTC)}',
        '{"electrical_heating"}',
        '{datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)}',
        {quantity}
      )
    """
    response_seed = databricks_api_client.execute_statement(statement=statement_seed, warehouse_id=config.warehouse_id)
    assert response_seed.result.row_count == 1

    # Act
    statement = f"""
                  SELECT *
                  FROM {config.catalog_name}.{current_v1.database_name}.{current_v1.view_name}
                  WHERE metering_point_id = '{170000030000000201}' and quantity = {quantity}
                  LIMIT 1
                """
    databricks_api_client.wait_for_data(statement=statement, warehouse_id=config.warehouse_id)
