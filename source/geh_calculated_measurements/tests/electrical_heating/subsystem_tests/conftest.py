import pytest
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from .fixtures.eletrical_heating_fixture import ElectricalHeatingFixture


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()  # type: ignore


@pytest.fixture(scope="session")
def databricks_api_client(environment_configuration: EnvironmentConfiguration) -> DatabricksApiClient:
    return DatabricksApiClient(
        environment_configuration.databricks_token,
        environment_configuration.workspace_url,
    )


@pytest.fixture(scope="session")
def electrical_heating_fixture(databricks_api_client: DatabricksApiClient) -> ElectricalHeatingFixture:
    return ElectricalHeatingFixture(databricks_api_client)
