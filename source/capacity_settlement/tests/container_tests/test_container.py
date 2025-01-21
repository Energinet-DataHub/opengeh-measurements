import pytest

from tests.container_tests.databricks_api_client import DatabricksApiClient


def test__databricks_job_starts_and_stops_successfully(
    databricks_client: DatabricksApiClient,
) -> None:
    """
    Tests that a Databricks capacity settlement job runs successfully to completion.
    """
    try:
        # TODO AJW Arrange - Change job id to an capacity settlement job id - currently it's an electrical heating job id
        job_id = 576172778546244

        # Act
        run_id = databricks_client.start_job(job_id)

        # Assert
        result = databricks_client.wait_for_job_completion(run_id)
        assert result == "SUCCESS", f"Job did not complete successfully: {result}"
    except Exception as e:
        pytest.fail(f"Databricks job test failed: {e}")
