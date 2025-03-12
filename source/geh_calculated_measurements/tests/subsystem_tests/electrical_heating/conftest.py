import pytest

from tests.subsystem_tests.electrical_heating.environment_configuration import EnvironmentConfiguration


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()
