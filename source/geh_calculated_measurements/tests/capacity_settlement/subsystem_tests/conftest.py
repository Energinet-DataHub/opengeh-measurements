import pytest

from tests.capacity_settlement.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.capacity_settlement.subsystem_tests.fixtures.capacity_settlement_fixture import CapacitySettlementFixture


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()  # type: ignore


@pytest.fixture(scope="session")
def capacity_settlement_fixture(environment_configuration: EnvironmentConfiguration) -> CapacitySettlementFixture:
    return CapacitySettlementFixture(environment_configuration)
