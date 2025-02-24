from unittest import mock

import pytest
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging

from tests import PROJECT_ROOT
from tests.capacity_settlement.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="session")
def env_args_fixture() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
    }
    return env_args


@pytest.fixture(scope="session")
def script_args_fixture() -> list[str]:
    sys_argv = [
        "program_name",
        "--force_configuration",
        "false",
        "--orchestration-instance-id",
        "4a540892-2c0a-46a9-9257-c4e13051d76a",
        "--calculation-month",
        "1",
        "--calculation-year",
        "2021",
    ]
    return sys_argv


@pytest.fixture(autouse=True)
def configure_dummy_logging(env_args_fixture, script_args_fixture) -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    # Command line arguments
    with (
        mock.patch("sys.argv", script_args_fixture),
        mock.patch.dict("os.environ", env_args_fixture, clear=False),
        mock.patch(
            "geh_common.telemetry.logging_configuration.configure_azure_monitor"
        ),  # Patching call to configure_azure_monitor in order to not actually connect to app. insights.
    ):
        logging_settings = LoggingSettings()
        configure_logging(logging_settings=logging_settings, extras=None)


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """
    Returns the source/contract folder path.
    Please note that this only works if current folder haven't been changed prior using
    `os.chdir()`. The correctness also relies on the prerequisite that this function is
    actually located in a file located directly in the tests folder.
    """
    return f"{PROJECT_ROOT}/src/geh_calculated_measurements/capacity_settlement/contracts"


@pytest.fixture(scope="session")
def test_session_configuration() -> TestSessionConfiguration:  # noqa: F821
    settings_file_path = PROJECT_ROOT / "tests" / "capacity_settlement" / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)
