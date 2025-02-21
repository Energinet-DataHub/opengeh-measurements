import time
import uuid
from datetime import timedelta

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryClient, LogsQueryPartialResult, LogsQueryResult, LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


class JobState:
    orchestrator_instance_id: str
    job_id: int
    run_id: int
    run_result_state: RunResultState


query = """INSERT INTO measurements (
  transaction_id, quantity, transaction_creation_datetime, created, modified, -- dynamic variables
  metering_point_id, observation_time, quality, metering_point_type -- static variables
)
SELECT
    REPLACE(CAST(uuid() AS VARCHAR(50)), '-', '') AS transaction_id, -- transaction_id
    CAST(RAND() * 1000000 AS DECIMAL(18, 3)) AS quantity, -- quantity
    GETDATE() AS transaction_creation_datetime, -- transaction_creation_datetime
    GETDATE() AS created, -- created
    GETDATE() AS modified, -- modified
    '170000030000000201' AS metering_point_id, -- metering_point_id
    '2024-11-30T23:00:00Z' AS observation_time, -- observation_time
    'measured' AS quality, -- quality
    'consumption' AS metering_point_type -- metering_point_type"""


class ElectricalHeatingFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration):
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        ).seed(
            warehouse_id=environment_configuration.warehouse_id,
            catalog=environment_configuration.catalog_name,
            schema=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
            statement=query,
        )
        self.job_state = JobState()
        self.credentials = DefaultAzureCredential()
        self.logs_query_client = LogsQueryClient(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id("ElectricalHeating")

    def start_job(self, job_id: int) -> int:
        self.job_state.orchestrator_instance_id = str(uuid.uuid4())
        params = [
            f"--orchestration-instance-id={self.job_state.orchestrator_instance_id}",
        ]
        return self.databricks_api_client.start_job(job_id, params)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(
        self, query: str, timeout_seconds: int = 1000, poll_interval_seconds: int = 5, timespan_minutes: int = 15
    ) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self._wait_for_condition(workspace_id, query, timeout_seconds, poll_interval_seconds, timespan_minutes)

    # TODO Move to the test common paakage in a future PR.
    def _wait_for_condition(
        self, workspace_id: str, query: str, timeout_seconds: int, poll_interval_seconds: int, timespan_minutes: int
    ) -> LogsQueryResult:
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                result = self.logs_query_client.query_workspace(
                    workspace_id, query, timespan=timedelta(timespan_minutes)
                )
                if result.status == LogsQueryStatus.SUCCESS and len(result.tables[0].rows) > 0:
                    return result
            except Exception:
                pass

            time.sleep(poll_interval_seconds)

        raise TimeoutError(f"Job did not complete within {timeout_seconds} seconds.")
