import time
import uuid
from datetime import timedelta

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryClient, LogsQueryPartialResult, LogsQueryResult, LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient


class CalculationInput:
    orchestrator_instance_id: uuid.UUID
    job_id: int
    year: int
    month: int


class JobState:
    run_id: int
    run_result_state: RunResultState
    calculation_input: CalculationInput = CalculationInput()


class CapacitySettlementFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration):
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.job_state = JobState()
        self.credentials = DefaultAzureCredential()
        self.logs_query_client = LogsQueryClient(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id("CapacitySettlement")

    def start_job(self, calculation_input: CalculationInput) -> int:
        params = [
            f"--orchestration-instance-id={str(calculation_input.orchestrator_instance_id)}",
            f"--calculation-month={calculation_input.month}",
            f"--calculation-year={calculation_input.year}",
        ]

        return self.databricks_api_client.start_job(calculation_input.job_id, params)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(self, query: str) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self._wait_for_condition(workspace_id, query)

    # TODO Move to the test common paakage in a future PR.
    def _wait_for_condition(
        self,
        workspace_id: str,
        query: str,
        timeout_seconds: int = 1000,
        poll_interval_seconds: int = 5,
        timespan_minutes: int = 15,
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
