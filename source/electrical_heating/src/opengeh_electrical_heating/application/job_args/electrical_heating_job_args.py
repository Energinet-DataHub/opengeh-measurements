import datetime
import sys
import uuid
from argparse import Namespace
from datetime import datetime
from typing import Optional, Tuple, Type

import configargparse
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource
from telemetry_logging import Logger, logging_configuration

from opengeh_electrical_heating.application.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from opengeh_electrical_heating.application.job_args.environment_variables import (
    get_catalog_name,
    get_time_zone,
)


class ElectricalHeatingJobArgs(BaseSettings):
    """ElectricalHeatingArgs to retrieve and validate parameters and environment variables automatically.

    Parameters can come from both runtime (CLI) or from environment variables.
    The priority is CLI parameters first and then environment variables.
    """

    catalog_name: str
    time_zone: str
    execution_start_datetime: Optional[datetime]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return CliSettingsSource(settings_cls, cli_parse_args=True, cli_ignore_unknown_args=True), env_settings


def parse_command_line_arguments() -> Namespace:
    return _parse_args_or_throw(sys.argv[1:])


def parse_job_arguments(
    job_args: Namespace,
) -> ElectricalHeatingArgs:
    logger = Logger(__name__)
    logger.info(f"Command line arguments: {repr(job_args)}")

    with logging_configuration.start_span("electrical_heating.parse_job_arguments"):
        electrical_heating_args = ElectricalHeatingArgs(
            catalog_name=get_catalog_name(),
            orchestration_instance_id=uuid.UUID(job_args.orchestration_instance_id),
            time_zone=get_time_zone(),
        )

    return electrical_heating_args


def _parse_args_or_throw(command_line_args: list[str]) -> Namespace:
    p = configargparse.ArgParser(
        description="Execute electrical heating calculation",
        formatter_class=configargparse.ArgumentDefaultsHelpFormatter,
    )

    # Run parameters
    p.add_argument("--orchestration-instance-id", type=str, required=True)

    args, unknown_args = p.parse_known_args(args=command_line_args)
    if len(unknown_args):
        unknown_args_text = ", ".join(unknown_args)
        raise Exception(f"Unknown args: {unknown_args_text}")

    return args


def valid_date(s: str) -> datetime:
    """See https://stackoverflow.com/questions/25470844/specify-date-format-for-python-argparse-input-arguments"""
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        msg = f"not a valid date: {s!r}"
        raise configargparse.ArgumentTypeError(msg)
