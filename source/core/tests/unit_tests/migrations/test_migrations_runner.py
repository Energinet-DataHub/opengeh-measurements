from pytest_mock import MockerFixture

import core.migrations.migrations_runner as sut


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
