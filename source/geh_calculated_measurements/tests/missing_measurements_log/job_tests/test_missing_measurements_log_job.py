import os
import uuid

from geh_calculated_measurements.missing_measurements_log.entry_point import execute
from tests import create_job_environment_variables


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
    monkeypatch,
    dummy_logging: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = uuid.uuid4()
    monkeypatch.setattr("sys.argv", _create_job_arguments(orchestration_instance_id))
    monkeypatch.setattr(os, "environ", create_job_environment_variables())

    # Act
    execute()

    # Assert
    # TODO: Assert that data is written in delta. For now the test only asserts that no exception is raised.
