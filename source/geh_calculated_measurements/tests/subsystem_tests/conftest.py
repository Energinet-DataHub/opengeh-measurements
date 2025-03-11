import pytest

from tests.subsystem_tests.electrical_heating.fixtures.eletrical_heating_fixture import ElectricalHeatingFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@pytest.fixture(scope="session")
def electrical_heating_fixture(environment_configuration: EnvironmentConfiguration) -> ElectricalHeatingFixture:
    return ElectricalHeatingFixture(environment_configuration)
