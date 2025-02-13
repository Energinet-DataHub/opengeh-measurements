import pytest
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test import DatabricksApiClient
from sql_seed_query import query

from src.geh_calculated_measurements.opengeh_electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()


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
