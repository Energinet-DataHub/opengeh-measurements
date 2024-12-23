import argparse
import sys
import configargparse
from argparse import Namespace
from telemetry_logging import Logger, logging_configuration

from .electrical_heating_args import ElectricalHeatingArgs
from electrical_heating import entry_points as env_vars


def parse_command_line_arguments() -> Namespace:
    return _parse_args_or_throw(sys.argv[1:])


def parse_job_arguments(
    job_args: Namespace,
) -> ElectricalHeatingArgs:
    logger = Logger(__name__)
    logger.info(f"Command line arguments: {repr(job_args)}")

    with logging_configuration.start_span("electrical_heating.parse_job_arguments"):

        electrical_heating_args = ElectricalHeatingArgs(
            catalog_name=env_vars.get_catalog_name(),
            orchestration_instance_id=job_args.orchestration_instance_id,
            time_zone=env_vars.get_time_zone(),
        )

        return electrical_heating_args


def _parse_args_or_throw(command_line_args: list[str]) -> argparse.Namespace:
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
