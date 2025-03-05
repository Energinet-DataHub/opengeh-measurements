from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient, PartitionContext
from azure.identity import DefaultAzureCredential
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings
from tests.subsystem_tests.settings.kafka_settings import KafkaSettings


class RunState:
    def __init__(self, run_id: int) -> None:
        self.run_id = run_id

    run_id: int
    run_result_state: RunResultState


class CoreFixture:
    def __init__(self, databrick_settings: DatabricksSettings) -> None:
        self.databricks_api_client = DatabricksApiClient(
            databrick_settings.token,
            databrick_settings.workspace_url,
        )
        self.run_states: list[RunState] = []

    def start_jobs(self) -> None:
        # Start "Bronze Submitted Transactions Ingestion Stream"
        # Start "Bronze Submitted Transactions to Silver Measurements"
        # Start "Silver Notify Transactions Persisted Stream"

        self.start_job("Bronze Submitted Transactions to Silver Measurements")
        self.start_job("Bronze Submitted Transactions Ingestion Stream")
        self.start_job("Silver Notify Transactions Persisted Stream")

    def start_job(self, job_name: str) -> None:
        job_id = self.databricks_api_client.get_job_id(job_name)
        run_id = self.databricks_api_client.start_job(job_id, [])

        self.run_states.append(RunState(run_id))

    def send_submitted_transactions_event(self, value) -> None:
        kafka_settings = KafkaSettings()  # type: ignore

        credential = DefaultAzureCredential()

        producer = EventHubProducerClient(
            fully_qualified_namespace=f"{kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=kafka_settings.event_hub_submitted_transactions_instance,
            credential=credential,
        )

        event_data_batch = producer.create_batch()

        event_data_batch.add(EventData(value))

        producer.send_batch(event_data_batch)

        producer.close()

    def assert_runs_are_running(self) -> None:
        for run_state in self.run_states:
            run = self.databricks_api_client.client.jobs.get_run(run_state.run_id)
            life_cycle_state = run.state.life_cycle_state.value  # type: ignore

            if life_cycle_state not in [RunLifeCycleState.RUNNING.value, RunLifeCycleState.PENDING.value]:
                raise AssertionError(f"Run {run_state.run_id} is not running. Current state: {life_cycle_state}")

    def assert_receipt(self) -> None:
        kafka_settings = KafkaSettings()  # type: ignore
        credential = DefaultAzureCredential()

        consumer = EventHubConsumerClient(
            fully_qualified_namespace=f"{kafka_settings.event_hub_namespace}.servicebus.windows.net",
            eventhub_name=kafka_settings.event_hub_receipt_instance,
            consumer_group="$Default",
            credential=credential,
        )

        def on_event(partition_context: PartitionContext, event: EventData | None):
            if event is None:
                print("No event received.")
                return

            # Print the event data.
            print(
                'Received the event: "{}" from the partition with ID: "{}"'.format(
                    event.body_as_str(encoding="UTF-8"), partition_context.partition_id
                )
            )

            assert event.body_as_str(encoding="UTF-8") is not None

        consumer.receive_batch(on_event=on_event, prefetch=10)
