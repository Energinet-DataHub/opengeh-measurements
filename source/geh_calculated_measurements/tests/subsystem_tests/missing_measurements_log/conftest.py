import pytest

from tests.subsystem_tests.capacity_settlement.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.capacity_settlement.fixtures.capacity_settlement_fixture import CapacitySettlementFixture


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()


@pytest.fixture(scope="session")
def capacity_settlement_fixture(environment_configuration: EnvironmentConfiguration) -> CapacitySettlementFixture:
    return CapacitySettlementFixture(environment_configuration)
