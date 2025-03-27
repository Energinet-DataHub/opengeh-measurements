import geh_common.telemetry.logging_configuration as config
import pytest
from pytest_mock import MockFixture


@pytest.fixture(scope="session")
def env_args_fixture_logging() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
    }
    return env_args


@pytest.fixture(scope="session")
def script_args_fixture_logging() -> list[str]:
    sys_argv = [
        "program_name",
        "--orchestration-instance-id",
        "00000000-0000-0000-0000-000000000001",
    ]
    return sys_argv


@pytest.fixture(autouse=True)
def configure_dummy_logging(mocker: MockFixture, env_args_fixture_logging, script_args_fixture_logging):
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""

    mocker.patch("sys.argv", script_args_fixture_logging)
    mocker.patch.dict("os.environ", env_args_fixture_logging, clear=False)
    mocker.patch(
        "geh_common.telemetry.logging_configuration.configure_azure_monitor"
    )  # Patching call to configure_azure_monitor in order to not actually connect to app. insights.
    logging_settings = config.LoggingSettings()  # type: ignore
    yield config.configure_logging(logging_settings=logging_settings, extras=None)  # type: ignore
