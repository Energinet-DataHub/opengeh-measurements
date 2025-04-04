from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then

import tests.helpers.identifier_helper as identifier_helper
from tests.helpers.builders.submitted_transactions_builder import ValueBuilder
from tests.subsystem_tests.fixtures.gold_layer_fixture import GoldLayerFixture
from tests.subsystem_tests.fixtures.kafka_fixture import KafkaFixture

scenarios("../features/transmission_of_measurements.feature")


class TestData:
    def __init__(self, orchestration_instance_id: str, value: str) -> None:
        self.orchestration_instance_id = orchestration_instance_id
        self.value = value


@given("a valid measurement transaction is enqueued in the Event Hub", target_fixture="test_data")
def _(spark: SparkSession, kafka_fixture: KafkaFixture) -> TestData:
    orchestration_instance_id = identifier_helper.generate_random_string()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    value = (
        ValueBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            metering_point_id=metering_point_id,
        )
        .build()
    )
    test_data = TestData(orchestration_instance_id, value)
    kafka_fixture.send_submitted_transactions_event(test_data.value)
    return test_data


@then("an acknowledgement is sent to the Event Hub")
def _(kafka_fixture: KafkaFixture, test_data: TestData) -> None:
    kafka_fixture.assert_receipt(test_data.orchestration_instance_id)


@then("the measurement transaction is available in the Gold Layer")
def _(gold_layer_fixture: GoldLayerFixture, test_data: TestData) -> None:
    gold_layer_fixture.assert_measurement_persisted(test_data.orchestration_instance_id)
