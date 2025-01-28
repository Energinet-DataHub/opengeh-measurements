import os
import time
import uuid

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState


class DatabricksApiClient:
    def __init__(self) -> None:
        databricks_token = os.getenv("DATABRICKS_TOKEN")
        databricks_host = os.getenv("WORKSPACE_URL")

        self.client = WorkspaceClient(host=databricks_host, token=databricks_token)

    def get_job_id(self, job_name: str) -> int:
        """
        Gets the job ID for a Databricks job.
        """
        job_list = self.client.jobs.list()
        for job in job_list:
            if job.settings is not None and job.settings.name == job_name:
                if job.job_id is not None:
                    return job.job_id
        raise Exception(f"Job '{job_name}' not found.")

    def start_job(self, job_id: int, python_params: list[str]) -> int:
        """
        Starts a Databricks job using the Databricks SDK and returns the run ID.
        """
        response = self.client.jobs.run_now(job_id=job_id, python_params=python_params)
        return response.run_id

    def wait_for_job_completion(self, run_id: int, timeout: int = 600, poll_interval: int = 10) -> RunResultState:
        """
        Waits for a Databricks job to complete.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            run_status = self.client.jobs.get_run(run_id=run_id)
            if run_status.state is None:
                raise Exception("Job run status state is None")

            lifecycle_state = run_status.state.life_cycle_state
            result_state = run_status.state.result_state

            if lifecycle_state == "TERMINATED":
                if result_state is None:
                    raise Exception("Job terminated but result state is None")
                return result_state
            elif lifecycle_state == "INTERNAL_ERROR":
                raise Exception(f"Job failed with an internal error: {run_status.state.state_message}")

            time.sleep(poll_interval)

        raise TimeoutError(f"Job did not complete within {timeout} seconds.")


def test__databricks_job_starts_and_stops_successfully() -> None:
    """
    Tests that a Databricks electrical heating job runs successfully to completion.
    """
    try:
        # Arrange
        databricksApiClient = DatabricksApiClient()
        job_id = databricksApiClient.get_job_id("ElectricalHeating")

        # Act
        run_id = databricksApiClient.start_job(
            job_id,
            [f"--orchestration-instance-id={str(uuid.uuid4())}", "--calculation-month=1", "--calculation-year=2024"],
        )

        # Assert
        result = databricksApiClient.wait_for_job_completion(run_id)
        assert result == "SUCCESS", f"Job did not complete successfully: {result}"
    except Exception as e:
        pytest.fail(f"Databricks job test failed: {e}")
  
