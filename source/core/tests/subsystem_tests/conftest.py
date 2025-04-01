import pytest

import tests.helpers.environment_variables_helpers as environment_variables_helpers
from tests.subsystem_tests.fixtures.gold_layer_fixture import GoldLayerFixture
from tests.subsystem_tests.fixtures.kafka_fixture import KafkaFixture


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    environment_variables_helpers.set_subsystem_test_environment_variables()


@pytest.fixture
def kafka_fixture() -> KafkaFixture:
    return KafkaFixture()


@pytest.fixture
def gold_layer_fixture() -> GoldLayerFixture:
    return GoldLayerFixture()
