from threading import Timer

from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient, PartitionContext
from azure.identity import DefaultAzureCredential
from geh_common.databricks.databricks_api_client import DatabricksApiClient

import core.contracts.process_manager.Brs021ForwardMeteredDataNotifyV1_pb2 as Brs021ForwardMeteredDataNotifyV1
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

    def assert_receipt(self, orchestration_instance_id: str) -> None:
        kafka_settings = KafkaSettings()  # type: ignore
        credential = DefaultAzureCredential()

        client = EventHubConsumerClient(
            fully_qualified_namespace=f"{kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=kafka_settings.event_hub_receipt_instance,
            consumer_group="measurements-subsystemtest",
            credential=credential,
        )

        def stop_receive(client):
            print("Stopping client after 1 minute")
            client.close()

        global found_orchestration_instance_id
        found_orchestration_instance_id = False

        # debug logging
        def on_event(partition_context: PartitionContext, event: EventData | None) -> None:
            global found_orchestration_instance_id
            if event is None:
                return
            body = event.body_as_str()
            message = Brs021ForwardMeteredDataNotifyV1.Brs021ForwardMeteredDataNotifyV1()
            body = body.encode("utf-8")
            message.ParseFromString(body)

            if message.orchestration_instance_id == orchestration_instance_id:
                found_orchestration_instance_id = True
                client.close()

        with client:
            timer = Timer(60, stop_receive, [client])
            timer.start()
            client.receive(on_event=on_event, max_wait_time=60)

        assert found_orchestration_instance_id is True
        credential.close()
