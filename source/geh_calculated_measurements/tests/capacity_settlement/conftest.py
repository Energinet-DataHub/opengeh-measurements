import os
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
    ):
        logging_settings = LoggingSettings()
        logging_settings.applicationinsights_connection_string = None  # for testing purposes
        configure_logging(logging_settings=logging_settings, extras=None)


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """
    Returns the source/contract folder path.
    Please note that this only works if current folder haven't been changed prior using
    `os.chdir()`. The correctness also relies on the prerequisite that this function is
    actually located in a file located directly in the tests folder.
    """
    return f"{PROJECT_ROOT}/src/geh_calculated_measurements/opengeh_capacity_settlement/contracts"


@pytest.fixture(scope="session")
def test_session_configuration() -> TestSessionConfiguration:  # noqa: F821
    settings_file_path = PROJECT_ROOT / "tests" / "capacity_settlement" / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


# https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
def pytest_collection_modifyitems(config, items) -> None:
    env_file_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_file_path):
        skip_container_tests = pytest.mark.skip(
            reason="Skipping container tests because .env file is missing. See .sample.env for an example."
        )
        for item in items:
            if "container_tests" in item.nodeid:
                item.add_marker(skip_container_tests)
