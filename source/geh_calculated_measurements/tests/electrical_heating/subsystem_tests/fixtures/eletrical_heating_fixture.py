import uuid

from databricks.sdk.service.jobs import RunResultState
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient


class JobState:
    job_id: int
    run_id: int
    run_result_state: RunResultState


class ElectricalHeatingFixture:
    job_state: JobState

    def __init__(self, databricks_api_client: DatabricksApiClient):
        self.databricks_api_client = databricks_api_client
        self.job_state = JobState()

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id("ElectricalHeating")

    def start_job(self, job_id: int) -> int:
        params = [
            f"--orchestration-instance-id={str(uuid.uuid4())}",
        ]
        return self.databricks_api_client.start_job(job_id, params)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)
