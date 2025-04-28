import random
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from databricks.sdk.service.sql import StatementState
from geh_common.data_products.measurements_core.measurements_gold import current_v1
from geh_common.databricks.databricks_api_client import DatabricksApiClient
from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

orchestration_instance_id = uuid.uuid4()
job_parameters = {"orchestration-instance-id": orchestration_instance_id}


def create_quantity(min=0.00, max=999999999999999.999) -> Decimal:
    return Decimal(random.uniform(min, max)).quantize(Decimal("1.000"))


def test__calculated_measurements_v1__is_usable_for_core(spark: SparkSession) -> None:
    """This test asserts that calculated measurements, which are provided by the data product will
    eventually be available in the core data product.

    Technically speaking, this implies that the data product supports streaming.

    The test always uses the same metering point. On each run, a new random quantity is
    used, which is used to assert that the data eventually is available in the core data product.
    """
    # Arrange
    config = EnvironmentConfiguration()

    quantity = create_quantity()

    databricks_api_client = DatabricksApiClient(config.databricks_token, config.workspace_url)

    database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
    table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME
    metering_point_id = 170000030000000201

    seed_statement = f"""
      INSERT INTO {config.catalog_name}.{database_name}.{table_name} VALUES
      (
        '{OrchestrationType.ELECTRICAL_HEATING.value}',
        '{orchestration_instance_id}',
        '{metering_point_id}',
        '{uuid.uuid4()}', -- transaction_id
        '{datetime.now(UTC)}', -- transaction_creation_datetime
        '{MeteringPointType.ELECTRICAL_HEATING.value}',
        '{datetime.now(UTC)}', -- observation_time - make sure this is the latest value as current_v1 only selects the latest
        {quantity}
      )
    """
    response_seed = databricks_api_client.execute_statement(statement=seed_statement, warehouse_id=config.warehouse_id)
    assert response_seed.result is not None
    assert response_seed.result.row_count == 1

    # Act
    assert_statement = f"""
                  SELECT *
                  FROM {config.catalog_name}.{current_v1.database_name}.{current_v1.view_name}
                  WHERE metering_point_id = '{metering_point_id}' and quantity = {quantity}
                  LIMIT 1
                """
    response = databricks_api_client.execute_statement(
        statement=assert_statement, warehouse_id=config.warehouse_id
    )

    # Assert
    assert response.status is not None, f"Expected statement to succeed, but got {response.status}"
    assert response.status.state == StatementState.SUCCEEDED, (
        f"Expected statement to succeed, but got {response.status.state}. Statement: {assert_statement}"
    )
