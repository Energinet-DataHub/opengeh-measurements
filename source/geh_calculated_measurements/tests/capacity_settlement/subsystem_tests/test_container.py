import uuid

import pytest
from geh_common.testing.container_test.databricks_api_client import DatabricksApiClient


def test__databricks_job_starts_and_stops_successfully(
    databricks_api_client: DatabricksApiClient,
) -> None:
    """
    Tests that a Databricks capacity settlement job runs successfully to completion.
    """
    try:
        # Arrange
        job_id = databricks_api_client.get_job_id("CapacitySettlement")

        # Act
        run_id = databricks_api_client.start_job(
            job_id,
            [
                f"--orchestration-instance-id={str(uuid.uuid4())}",
                "--calculation-month=1",
                "--calculation-year=2024",
            ],
        )

        # Assert
        result = databricks_api_client.wait_for_job_completion(run_id)
        assert result.value == "SUCCESS", f"Job did not complete successfully: {result.value}"
    except Exception as e:
        pytest.fail(f"Databricks job test failed: {e}")


def test__databricks_insert_statement(databricks_api_client: DatabricksApiClient) -> None:
    workspace_client = databricks_api_client.client

    # Table configuration
    catalog_name = "ctl_shres_d_we_002"
    schema_name = "measurements_gold"
    table_name = "measurements"

    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    insert_stmt = f"""
        INSERT INTO {full_table_name} VALUES
        ('test',2025-13-02,1.1,'Medium','test','test',2025-13-02,2025-13-02,2025-13-02)
    """

    workspace_client.statement_execution.execute_statement(
        catalog=catalog_name,
        schema=schema_name,
        statement=insert_stmt,
        warehouse_id="37a9cb56edb6529b",
    )
