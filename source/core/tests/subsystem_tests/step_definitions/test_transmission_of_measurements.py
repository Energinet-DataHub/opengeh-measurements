from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when

import tests.helpers.identifier_helper as identifier_helper
from tests.helpers.builders.submitted_transactions_builder import ValueBuilder
from tests.subsystem_tests.fixtures.kafka_fixture import KafkaFixture

scenarios("../features/transmission_of_measurements.feature")


class TestData:
    def __init__(self, orchestration_instance_id: str, value: str) -> None:
        self.orchestration_instance_id = orchestration_instance_id
        self.value = value


@given("a valid measurement transaction", target_fixture="test_data")
def _(spark: SparkSession) -> TestData:
    orchestration_instance_id = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    return TestData(orchestration_instance_id, value)


@when("the measurement transaction is enqueued in the Event Hub")
def _(core_fixture: KafkaFixture, test_data: TestData) -> None:
    core_fixture.send_submitted_transactions_event(test_data.value)


@then("an acknowledgement is sent to the Event Hub")
def _(core_fixture: KafkaFixture, test_data: TestData) -> None:
    core_fixture.assert_receipt(test_data.orchestration_instance_id)
