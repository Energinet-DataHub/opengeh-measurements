import pytest
from environment_configuration import EnvironmentConfiguration
from fixtures.eletrical_heating_fixture import ElectricalHeatingFixture
from fixtures.eletrical_heating_net_consumption_for_group_6_fixture import (
    ElectricalHeatingNetConsumptionForGroup6Fixture,
)


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()  # type: ignore


@pytest.fixture(scope="session")
def electrical_heating_fixture(environment_configuration: EnvironmentConfiguration) -> ElectricalHeatingFixture:
    return ElectricalHeatingFixture(environment_configuration)


@pytest.fixture(scope="session")
def electrical_heating_net_consumption_for_group_6_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> ElectricalHeatingNetConsumptionForGroup6Fixture:
    return ElectricalHeatingNetConsumptionForGroup6Fixture(environment_configuration)
