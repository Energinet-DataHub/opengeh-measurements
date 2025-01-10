import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState


class DatabricksApiClient:

    def __init__(self, host: str | None, token: str | None):
        self.client = WorkspaceClient(host=host, token=token)

    def start_job(self, job_id: int) -> int:
        """
        Starts a Databricks job using the Databricks SDK and returns the run ID.
        """
        response = self.client.jobs.run_now(job_id=job_id)
        return response.run_id

    def wait_for_job_completion(
        self, run_id: int, timeout: int = 300, poll_interval: int = 10
    ) -> RunResultState:
        """
        Waits for a Databricks job to complete.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            run_status = self.client.jobs.get_run(run_id=run_id)
            lifecycle_state = run_status.state.life_cycle_state
            result_state = run_status.state.result_state

            if lifecycle_state == "TERMINATED":
                return result_state
            elif lifecycle_state == "INTERNAL_ERROR":
                raise Exception(
                    f"Job failed with an internal error: {run_status.state.state_message}"
                )

            time.sleep(poll_interval)

        raise TimeoutError(f"Job did not complete within {timeout} seconds.")
