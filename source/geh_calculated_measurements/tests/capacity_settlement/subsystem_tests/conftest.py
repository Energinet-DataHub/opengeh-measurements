import pytest
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from tests.environment_configuration import EnvironmentConfiguration


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()  # type: ignore


@pytest.fixture(scope="session")
def databricks_api_client(
    environment_configuration: EnvironmentConfiguration,
) -> DatabricksApiClient:
    databricksApiClient = DatabricksApiClient(
        environment_configuration.databricks_token,
        environment_configuration.workspace_url,
    )
    return databricksApiClient
