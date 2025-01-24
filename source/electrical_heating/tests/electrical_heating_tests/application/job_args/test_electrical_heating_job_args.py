from datetime import datetime
from unittest.mock import patch

import pytest

from electrical_heating.application.job_args.electrical_heating_job_args import (
    parse_command_line_arguments,
    parse_job_arguments,
)
from electrical_heating.application.job_args.environment_variables import (
    EnvironmentVariable,
)

DEFAULT_ORCHESTRATION_INSTANCE_ID = "12345678-9fc8-409a-a169-fbd49479d711"
DEFAULT_PERIOD_START = datetime(2024, 2, 1, 23)
DEFAULT_PERIOD_END = datetime(2024, 3, 1, 23)


def _get_contract_parameters(filename: str) -> list[str]:
    with open(filename) as file:
        text = file.read()
        text = text.replace("{orchestration-instance-id}", DEFAULT_ORCHESTRATION_INSTANCE_ID)
        text = text.replace("{period-start}", DEFAULT_PERIOD_START.strftime("%Y-%m-%dT%H:%M:%SZ"))
        text = text.replace("{period-end}", DEFAULT_PERIOD_END.strftime("%Y-%m-%dT%H:%M:%SZ"))
        lines = text.splitlines()
        return list(filter(lambda line: not line.startswith("#") and len(line) > 0, lines))


@pytest.fixture(scope="session")
def contract_parameters(contracts_path: str) -> list[str]:
    job_parameters = _get_contract_parameters(f"{contracts_path}/parameters-reference.txt")
    return job_parameters


@pytest.fixture(scope="session")
def sys_argv_from_contract(
    contract_parameters: list[str],
) -> list[str]:
    return ["dummy_script_name"] + contract_parameters


@pytest.fixture(scope="session")
def job_environment_variables() -> dict:
    return {
        EnvironmentVariable.CATALOG_NAME.name: "some_catalog",
        EnvironmentVariable.TIME_ZONE.name: "some_time_zone",
    }


def test_when_parameters__parses_parameters_from_contract(
    job_environment_variables: dict,
    sys_argv_from_contract: list[str],
) -> None:
    """
    This test ensures that the job accepts
    the arguments that are provided by the client.
    """
    # Arrange
    with patch("sys.argv", sys_argv_from_contract):
        with patch.dict("os.environ", job_environment_variables):
            command_line_args = parse_command_line_arguments()
            # Act
            actual_args = parse_job_arguments(command_line_args)

    # Assert
    assert actual_args.orchestration_instance_id == DEFAULT_ORCHESTRATION_INSTANCE_ID
    assert actual_args.period_start == DEFAULT_PERIOD_START
    assert actual_args.period_end == DEFAULT_PERIOD_END
