import pytest
from environment_configuration import EnvironmentConfiguration
from fixtures.capacity_settlement_fixture import CapacitySettlementFixture


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()  # type: ignore


@pytest.fixture(scope="session")
def capacity_settlement_fixture(environment_configuration: EnvironmentConfiguration) -> CapacitySettlementFixture:
    return CapacitySettlementFixture(environment_configuration)
