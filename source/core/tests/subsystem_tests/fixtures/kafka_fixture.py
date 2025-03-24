from threading import Timer

from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient, PartitionContext
from azure.identity import DefaultAzureCredential

import core.contracts.process_manager.Brs021ForwardMeteredDataNotifyV1_pb2 as Brs021ForwardMeteredDataNotifyV1
from tests.subsystem_tests.settings.kafka_settings import KafkaSettings


class KafkaFixture:
    def __init__(self) -> None:
        self.kafka_settings = KafkaSettings()  # type: ignore
        self.credential = DefaultAzureCredential()

    def send_submitted_transactions_event(self, value) -> None:
        producer = EventHubProducerClient(
            fully_qualified_namespace=f"{self.kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=self.kafka_settings.event_hub_submitted_transactions_instance,
            credential=self.credential,
        )

        with producer:
            event_data_batch = producer.create_batch()

            event_data_batch.add(EventData(value))

            producer.send_batch(event_data_batch)

            self.credential.close()

    def assert_receipt(self, orchestration_instance_id: str) -> None:
        client = EventHubConsumerClient(
            fully_qualified_namespace=f"{self.kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=self.kafka_settings.event_hub_receipt_instance,
            consumer_group="measurements-subsystemtest",
            credential=self.credential,
        )

        def stop_receive(client: EventHubConsumerClient) -> None:
            client.close()

        global found_orchestration_instance_id
        found_orchestration_instance_id = False

        def on_event(partition_context: PartitionContext, event: EventData | None) -> None:
            global found_orchestration_instance_id
            if event is None:
                return
            body = event.body_as_str()
            body = body.encode("utf-8")
            message = Brs021ForwardMeteredDataNotifyV1.Brs021ForwardMeteredDataNotifyV1()
            message.ParseFromString(body)

            if message.orchestration_instance_id == orchestration_instance_id:
                found_orchestration_instance_id = True
                client.close()

        with client:
            timer = Timer(300, stop_receive, [client])
            timer.start()
            client.receive(on_event=on_event, max_wait_time=60)
            timer.cancel()

        assert found_orchestration_instance_id is True
        self.credential.close()
