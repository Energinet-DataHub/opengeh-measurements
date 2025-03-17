from dataclasses import dataclass, field
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from geh_common.databricks.databricks_api_client import DatabricksApiClient

from geh_calculated_measurements.testing import LogQueryClientWrapper
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@dataclass
class CalculationInput:
    params: dict
    job_id: int


@dataclass
class JobState:
    run_id: int
    calculation_input: CalculationInput
    run_result_state: Optional[RunResultState] = field(default=None)


class BaseJobFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration, job_name: str, params: dict):
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )

        self.job_state = None
        self.calculation_input = None
        self.credentials = DefaultAzureCredential()
        self.azure_logs_query_client = LogQueryClientWrapper(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )
        self.job_name = job_name
        self.params = params

    def create_job_state(self, run_id, run_result_state, calculation_input) -> None:
        self.job_state = JobState(run_id, run_result_state, calculation_input)

    def create_calculation_input(self) -> None:
        job_id = self.get_job_id()
        self.calculation_input = CalculationInput(self.params, job_id)

    def set_run_result_state(self, run_result_state) -> None:
        self.job_state.run_result_state = run_result_state

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id(self.job_name)

    def start_job(self, calculation_input: CalculationInput) -> int:
        params_list = []
        if calculation_input.params:
            for key, value in calculation_input.params.items():
                params_list.append(f"--{key}={value}")
        return self.databricks_api_client.start_job(calculation_input.job_id, params_list)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(self, query: str) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)
