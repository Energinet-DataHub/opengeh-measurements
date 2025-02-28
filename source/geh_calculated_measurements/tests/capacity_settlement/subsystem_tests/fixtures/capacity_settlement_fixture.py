import uuid

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from tests.capacity_settlement.subsystem_tests.fixtures.log_query_client_wrapper import LogQueryClientWrapper


class CalculationInput:
    orchestration_instance_id: uuid.UUID
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
        self.azure_logs_query_client = LogQueryClientWrapper(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id("CapacitySettlement")

    def start_job(self, calculation_input: CalculationInput) -> int:
        params = [
            f"--orchestration-instance-id={str(calculation_input.orchestration_instance_id)}",
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

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)
