from pytest_mock import MockerFixture

import core.databases.migrations.migrations_runner as sut


def test__migrate__calls_expected(mocker: MockerFixture):
    mock_configure_spark_sql_migrations = mocker.patch.object(
        sut,
        sut._configure_spark_sql_migrations.__name__,
    )
    mock_stop_job_runs = mocker.patch(
        f"{sut.__name__}._stop_job_runs",
    )
    mock_migrate_pipeline = mocker.patch.object(
        sut.migration_pipeline,
        sut.migration_pipeline.migrate.__name__,
    )

    # Act
    sut.migrate()

    # Assert
    mock_configure_spark_sql_migrations.assert_called_once()
    mock_stop_job_runs.assert_called_once()
    mock_migrate_pipeline.assert_called_once()


def test__stop_job_runs__calls_expected(mocker: MockerFixture):
    # Arrange
    mock_databricks_settings = mocker.patch(f"{sut.__name__}.DatabricksSettings")
    mock_databricks_api_client = mocker.patch(f"{sut.__name__}.DatabricksApiClient")
    mock_settings_instance = mock_databricks_settings.return_value
    mock_settings_instance.databricks_jobs = "job1,job2"
    mock_client_instance = mock_databricks_api_client.return_value

    # Act
    sut._stop_job_runs()

    # Assert
    mock_databricks_api_client.assert_called_once_with(
        databricks_host=mock_settings_instance.databricks_workspace_url,
        databricks_token=mock_settings_instance.databricks_token,
    )
    assert mock_client_instance.get_job_id.call_count == 2
    assert mock_client_instance.get_latest_job_run.job_run_id.call_count == 2
    assert mock_client_instance.cancel_job_run.call_count == 2
