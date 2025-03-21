import pytest

from tests.subsystem_tests.fixtures.kafka_fixture import KafkaFixture


@pytest.fixture
def core_fixture() -> KafkaFixture:
    return KafkaFixture()
