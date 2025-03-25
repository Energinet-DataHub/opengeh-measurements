from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from geh_common.databricks.databricks_api_client import DatabricksApiClient

from geh_calculated_measurements.testing import LogQueryClientWrapper
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class BaseJobFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration, job_name: str, job_parameters: dict):
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )

        self.credentials = DefaultAzureCredential()
        self.azure_logs_query_client = LogQueryClientWrapper(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )
        self.job_name = job_name
        self.job_parameters = job_parameters
        self.environment_configuration = environment_configuration

    def set_run_id(self, run_id: int) -> None:
        self.run_id = run_id

    def get_run_id(self) -> int | None:
        return self.run_id

    def start_job(self) -> int:
        job_id = self.databricks_api_client.get_job_id(self.job_name)
        params_list = []
        if self.job_parameters:
            for key, value in self.job_parameters.items():
                params_list.append(f"--{key}={value}")

        run_id = self.databricks_api_client.start_job(job_id, params_list)

        return run_id

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(self, query: str) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)
