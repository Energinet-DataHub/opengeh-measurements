import pytest

from tests.subsystem_tests.missing_measurements_log.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.missing_measurements_log.fixtures.missing_measurements_log_fixture import (
    MissingMeasurementsLogFixture,
)


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()


@pytest.fixture(scope="session")
def missing_measurements_log_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> MissingMeasurementsLogFixture:
    return MissingMeasurementsLogFixture(environment_configuration)
