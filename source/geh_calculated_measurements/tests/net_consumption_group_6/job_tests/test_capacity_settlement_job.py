import uuid
from unittest.mock import patch

from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from tests.net_consumption_group_6.job_tests import create_job_environment_variables


def _create_job_parameters(orchestration_instance_id: str) -> list[str]:
    return [
        "dummy_script_name",
        f"--orchestration-instance-id={orchestration_instance_id}",
        "--calculation-year=2026",
        "--calculation-month=1",
    ]


def test_execute() -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())

    with patch("sys.argv", _create_job_parameters(orchestration_instance_id)):
        with patch.dict("os.environ", create_job_environment_variables()):
            # Act
            execute()

    # Assert
    # Later stories will eventually add assertions about results being stored
