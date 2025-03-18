from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient, PartitionContext
from azure.identity import DefaultAzureCredential
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings
from tests.subsystem_tests.settings.kafka_settings import KafkaSettings


class CoreFixture:
    def __init__(self, databrick_settings: DatabricksSettings) -> None:
        self.databricks_api_client = DatabricksApiClient(
            databrick_settings.token,
            databrick_settings.workspace_url,
        )

    def send_submitted_transactions_event(self, value) -> None:
        kafka_settings = KafkaSettings()  # type: ignore

        credential = DefaultAzureCredential()

        producer = EventHubProducerClient(
            fully_qualified_namespace=f"{kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=kafka_settings.event_hub_submitted_transactions_instance,
            credential=credential,
        )

        with producer:
            event_data_batch = producer.create_batch()

            event_data_batch.add(EventData(value))

            producer.send_batch(event_data_batch)

            credential.close()

    def assert_receipt(self) -> None:
        kafka_settings = KafkaSettings()  # type: ignore
        credential = DefaultAzureCredential()

        consumer = EventHubConsumerClient(
            fully_qualified_namespace=f"{kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=kafka_settings.event_hub_receipt_instance,
            consumer_group="$Default",
            credential=credential,
        )

        # debug logging
        def on_event_batch(partition_context: PartitionContext, events: list[EventData]) -> None:
            print("DEBUG: partition_context: {}".format(partition_context))
            print("DEBUG: events: {}".format(events))

        with consumer:
            print("DEBUG: Receiving events")
            consumer.receive_batch(on_event_batch=on_event_batch)

        credential.close()
