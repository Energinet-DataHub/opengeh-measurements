from unittest.mock import patch

from core.migrations.migrations_runner import _start_jobs, _stop_job_runs, migrate


@patch("core.migrations.migrations_runner._configure_spark_sql_migrations")
@patch("core.migrations.migrations_runner._start_jobs")
@patch("core.migrations.migrations_runner._stop_job_runs")
@patch("core.migrations.migrations_runner.migration_pipeline.migrate")
def test__migrate__calls_expected(
    mock_migrate_pipeline, mock_stop_job_runs, mock_start_jobs, mock_configure_spark_sql_migrations
):
    # Act
    migrate()

    # Assert
    mock_configure_spark_sql_migrations.assert_called_once()
    mock_stop_job_runs.assert_called_once()
    mock_migrate_pipeline.assert_called_once()
    mock_start_jobs.assert_called_once()


@patch("core.migrations.migrations_runner.DatabricksApiClient")
@patch("core.migrations.migrations_runner.DatabricksSettings")
def test__stop_job_runs__calls_expected(mock_databricks_settings, mock_databricks_api_client):
    # Arrange
    mock_settings_instance = mock_databricks_settings.return_value
    mock_settings_instance.databricks_jobs = "job1,job2"
    mock_client_instance = mock_databricks_api_client.return_value

    # Act
    _stop_job_runs()

    # Assert
    mock_databricks_api_client.assert_called_once_with(
        databricks_host=mock_settings_instance.databricks_workspace_url,
        databricks_token=mock_settings_instance.databricks_token,
    )
    assert mock_client_instance.get_job_id.call_count == 2
    assert mock_client_instance.get_latest_job_run_id.call_count == 2
    assert mock_client_instance.cancel_job_run.call_count == 2


@patch("core.migrations.migrations_runner.DatabricksApiClient")
@patch("core.migrations.migrations_runner.DatabricksSettings")
def test__start_jobs__calls_expected(mock_databricks_settings, mock_databricks_api_client):
    # Arrange
    mock_settings_instance = mock_databricks_settings.return_value
    mock_settings_instance.databricks_jobs = "job1,job2"
    mock_client_instance = mock_databricks_api_client.return_value

    # Act
    _start_jobs()

    # Assert
    mock_databricks_api_client.assert_called_once_with(
        databricks_host=mock_settings_instance.databricks_workspace_url,
        databricks_token=mock_settings_instance.databricks_token,
    )
    assert mock_client_instance.get_job_id.call_count == 2
    assert mock_client_instance.start_job.call_count == 2
