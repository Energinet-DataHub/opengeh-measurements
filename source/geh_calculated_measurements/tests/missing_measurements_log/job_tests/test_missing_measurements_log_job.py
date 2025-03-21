import uuid

from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.entry_point import execute


def _create_job_environment_variables() -> dict[str, str]:
    return {
        "CATALOG_NAME": "spark_catalog",
        "TIME_ZONE": "Europe/Copenhagen",
    }


def _create_job_arguments(orchestration_instance_id: uuid.UUID) -> list[str]:
    return [
        "dummy_script_name",
        "--orchestration-instance-id",
        str(orchestration_instance_id),
        "--period-start-datetime",
        "2025-01-02T22:00:00Z",
        "--period-end-datetime",
        "2025-01-03T22:00:00Z",
    ]


def test_execute(
    spark: SparkSession,
    monkeypatch,
) -> None:
    # Arrange
    orchestration_instance_id = uuid.uuid4()
    monkeypatch.setattr("sys.argv", _create_job_arguments(orchestration_instance_id))
    monkeypatch.setattr("os.environ", _create_job_environment_variables())

    # Act
    execute()

    # Assert
    # TODO: Assert that data is written in delta. For now the test only asserts that no exception is raised.
