import pytest

from tests.subsystem_tests.fixtures.gold_layer_fixture import GoldLayerFixture
from tests.subsystem_tests.fixtures.kafka_fixture import KafkaFixture


@pytest.fixture
def kafka_fixture() -> KafkaFixture:
    return KafkaFixture()


@pytest.fixture
def gold_layer_fixture() -> GoldLayerFixture:
    return GoldLayerFixture()
