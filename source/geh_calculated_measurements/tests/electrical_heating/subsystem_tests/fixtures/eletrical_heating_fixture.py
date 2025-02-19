import uuid

from databricks.sdk.service.jobs import RunResultState
from environment_configuration import EnvironmentConfiguration
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient


class JobState:
    job_id: int
    run_id: int
    run_result_state: RunResultState


class ElectricalHeatingFixture:
    job_state: JobState

    def __init__(self, databricks_api_client: DatabricksApiClient, environment_configuration: EnvironmentConfiguration):
        self.databricks_api_client = databricks_api_client
        self.environment_configuration = environment_configuration
        self.job_state = JobState()

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id("ElectricalHeating")

    def start_job(self, job_id: int, environment_configuration: EnvironmentConfiguration) -> int:
        params = [
            f"--orchestration-instance-id={str(uuid.uuid4())}",
            f"--catalog-name={environment_configuration.catalog_name}",
            f"--schema-name={environment_configuration.schema_name}",
            f"--time-series-points-table={environment_configuration.time_series_points_table}",
            f"--consumption-points-table={environment_configuration.consumption_points_table}",
            f"--child-points-table={environment_configuration.child_points_table}",
        ]
        return self.databricks_api_client.start_job(job_id, params)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)
