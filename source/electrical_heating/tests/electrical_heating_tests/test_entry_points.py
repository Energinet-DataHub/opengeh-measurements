from unittest import mock

import tomli

from opengeh_electrical_heating import entry_point
from tests import PROJECT_ROOT


def test__entry_point_exists() -> None:
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as file:
        pyproject = tomli.load(file)
        project = pyproject.get("project", {})
    scripts = project.get("scripts", {})
    assert "execute" in scripts, "`execute` not found in scripts"


def test__execute() -> None:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
        "ORCHESTRATION_INSTANCE_ID": "4a540892-2c0a-46a9-9257-c4e13051d76b",
    }
    with (
        mock.patch(
            "sys.argv",
            [
                "program_name",
                "--force_configuration",
                "false",
                "--orchestration_instance_id",
                "4a540892-2c0a-46a9-9257-c4e13051d76a",
            ],
        ),
        mock.patch.dict("os.environ", env_args, clear=False),
        mock.patch("electrical_heating.ElectricalHeatingJobArgs", autospec=True) as mock_job_args,
        mock.patch("config.LoggingSettings", autospec=True) as mock_logging_settings,
        mock.patch("config.configure_logging") as mock_configure_logging,
        mock.patch("config.add_extras") as mock_add_extras,
        mock.patch("logger.Logger", autospec=True) as mock_logger,
        mock.patch("execute_with_deps") as mock_execute_with_deps,
    ):
        mock_logger_instance = mock_logger.return_value
        # Act
        entry_point.execute()

        # assert
        mock_logging_settings.assert_called_once()
        mock_configure_logging.assert_called_once_with(logging_settings=mock_logging_settings.return_value)
        mock_add_extras.assert_called_once_with({"tracer_name": "TRACER_NAME"})
        mock_logger.assert_called_once_with(__name__)
        mock_logger_instance.info.assert_any_call(
            f"Command line arguments / env variables retrieved for Logging Settings: {mock_logging_settings.return_value}"
        )
        mock_logger_instance.info.assert_any_call(
            f"Command line arguments retrieved for electrical heating job Oriented Parameters: {mock_job_args.return_value}"
        )
        mock_execute_with_deps.assert_called_once()
