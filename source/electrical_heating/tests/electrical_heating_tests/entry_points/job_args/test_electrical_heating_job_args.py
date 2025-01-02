import pytest

from electrical_heating.infrastructure.environment_variables import (
    EnvironmentVariable,
)
from unittest.mock import patch

from electrical_heating.main import execute

DEFAULT_ORCHESTRATION_INSTANCE_ID = "12345678-9fc8-409a-a169-fbd49479d711"


def _get_contract_parameters(filename: str) -> list[str]:
    with open(filename) as file:
        text = file.read()
        text = text.replace(
            "{orchestration-instance-id}", DEFAULT_ORCHESTRATION_INSTANCE_ID
        )
        lines = text.splitlines()
        return list(
            filter(lambda line: not line.startswith("#") and len(line) > 0, lines)
        )


@pytest.fixture(scope="session")
def contract_parameters(contracts_path: str) -> list[str]:
    job_parameters = _get_contract_parameters(
        f"{contracts_path}/parameters-reference.txt"
    )

    return job_parameters


@pytest.fixture(scope="session")
def sys_argv_from_contract(
    contract_parameters: list[str],
) -> list[str]:
    return ["dummy_script_name"] + contract_parameters


@pytest.fixture(scope="session")
def job_environment_variables() -> dict:
    return {
        EnvironmentVariable.CATALOG_NAME: "some_catalog",
        EnvironmentVariable.TIME_ZONE: "some_time_zone",
    }


def test_when_parameters__parses_parameters_from_contract(
    job_environment_variables: dict,
    sys_argv_from_contract: list[str],
) -> None:
    """
    This test ensures that the job accepts the command line arguments and passes them to the execute function.
    """
    # Arrange
    with patch("sys.argv", sys_argv_from_contract):
        with patch("electrical_heating.main._execute") as mock_execute:
            # Act
            execute()

    # Assert
    mock_execute.assert_called_once_with(DEFAULT_ORCHESTRATION_INSTANCE_ID)
