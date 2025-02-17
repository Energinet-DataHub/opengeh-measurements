import uuid
from datetime import timedelta

from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient

from geh_calculated_measurements.opengeh_electrical_heating.domain import ColumnNames


class JobState:
    orchestrator_instance_id: str
    job_id: int
    run_id: int
    run_result_state: RunResultState


class ElectricalHeatingFixture:
    job_state: JobState

    def __init__(self, environment_configuration: EnvironmentConfiguration):
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.job_state = JobState()
        self.client = LogsQueryClient(DefaultAzureCredential())
        self.WORKSPACE_ID = environment_configuration.workspace_id

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id("ElectricalHeating")

    def start_job(self, job_id: int) -> int:
        self.job_state.orchestrator_instance_id = str(uuid.uuid4())
        params = [
            f"--{ColumnNames.orchestration_instance_id}={self.job_state.orchestrator_instance_id}",
        ]
        return self.databricks_api_client.start_job(job_id, params)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def query_workspace_logs(self, query: str, timeout: int = 5) -> LogsQueryResult | LogsQueryPartialResult:
        return self.client.query_workspace(self.WORKSPACE_ID, query, timespan=timedelta(minutes=timeout))
