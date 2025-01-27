from unittest.mock import patch

import pytest

import electrical_heating.application.execute_with_deps as execute_with_deps
from electrical_heating.application.job_args.environment_variables import EnvironmentVariable

DEFAULT_ORCHESTRATION_INSTANCE_ID = "12345678-9fc8-409a-a169-fbd49479d711"


@pytest.fixture(scope="session")
def job_environment_variables() -> dict:
    return {
        EnvironmentVariable.CATALOG_NAME.name: "spark_catalog",
        EnvironmentVariable.TIME_ZONE.name: "Europe/Copenhagen",
    }


def test_execute_with_deps(
    job_environment_variables: dict,
):
    # Arrange
    sys_argv = ["dummy_script_name", "orchestration_instance_id", DEFAULT_ORCHESTRATION_INSTANCE_ID]

    # Act
    with patch("sys.argv", sys_argv):
        with patch.dict("os.environ", job_environment_variables):
            execute_with_deps.execute_with_deps()

    # Assert
