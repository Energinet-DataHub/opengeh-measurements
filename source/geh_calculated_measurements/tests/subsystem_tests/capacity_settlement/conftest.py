import pytest

from tests.subsystem_tests.capacity_settlement.fixtures.capacity_settlement_fixture import CapacitySettlementFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@pytest.fixture(scope="session")
def capacity_settlement_fixture(environment_configuration: EnvironmentConfiguration) -> CapacitySettlementFixture:
    return CapacitySettlementFixture(environment_configuration)
