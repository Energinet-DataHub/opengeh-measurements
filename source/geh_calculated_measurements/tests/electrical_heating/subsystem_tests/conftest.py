import pytest
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test import DatabricksApiClient

from src.geh_calculated_measurements.opengeh_electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()


query = """INSERT INTO measurements (
  transaction_id, quantity, transaction_creation_datetime, created, modified, -- dynamic variables
  metering_point_id, observation_time, quality, metering_point_type -- static variables
)
SELECT
    REPLACE(CAST(uuid() AS VARCHAR(50)), '-', '') AS transaction_id, -- transaction_id
    CAST(RAND() * 1000000 AS DECIMAL(18, 3)) AS quantity, -- quantity
    GETDATE() AS transaction_creation_datetime, -- transaction_creation_datetime
    GETDATE() AS created, -- created
    GETDATE() AS modified, -- modified
    '170000030000000201' AS metering_point_id, -- metering_point_id
    '2024-11-30T23:00:00Z' AS observation_time, -- observation_time
    'measured' AS quality, -- quality
    'consumption' AS metering_point_type -- metering_point_type"""


@pytest.fixture(scope="session")
def databricks_api_client(
    environment_configuration: EnvironmentConfiguration,
) -> DatabricksApiClient:
    databricksApiClient = (
        DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        ).seed(
            warehouse_id=environment_configuration.warehouse_id,
            catalog=environment_configuration.catalog_name,
            schema=MeasurementsGoldDatabase.DATABASE_NAME,
            statement=query,
        ),
    )

    return databricksApiClient
