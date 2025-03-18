import pytest

from tests.subsystem_tests.electrical_heating.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.electrical_heating.fixtures.eletrical_heating_fixture import ElectricalHeatingFixture


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()


@pytest.fixture(scope="session")
def electrical_heating_fixture(environment_configuration: EnvironmentConfiguration) -> ElectricalHeatingFixture:
    return ElectricalHeatingFixture(environment_configuration)
