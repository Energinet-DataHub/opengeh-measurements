import argparse
import sys
import uuid
from argparse import Namespace

import configargparse
from geh_common.telemetry import logging_configuration
from geh_common.telemetry.logger import Logger

from geh_calculated_measurements.capacity_settlement.application import (
    CapacitySettlementArgs,
)
from geh_calculated_measurements.capacity_settlement.application.job_args.environment_variables import get_catalog_name


def parse_command_line_arguments() -> Namespace:
    return _parse_args_or_throw(sys.argv[1:])


def parse_job_arguments(
    job_args: Namespace,
) -> CapacitySettlementArgs:
    logger = Logger(__name__)
    logger.info(f"Command line arguments: {repr(job_args)}")

    with logging_configuration.start_span("capacity_settlement.parse_job_arguments"):
        capacity_settlement_args = CapacitySettlementArgs(
            catalog_name=get_catalog_name(),
            orchestration_instance_id=uuid.UUID(job_args.orchestration_instance_id),
            calculation_month=job_args.calculation_month,
            calculation_year=job_args.calculation_year,
        )

        return capacity_settlement_args


def _parse_args_or_throw(command_line_args: list[str]) -> argparse.Namespace:
    p = configargparse.ArgParser(
        description="Execute capacity settlement calculation",
        formatter_class=configargparse.ArgumentDefaultsHelpFormatter,
    )

    # Run parameters
    p.add_argument("--orchestration-instance-id", type=str, required=True)
    p.add_argument("--calculation-month", type=int, required=True)
    p.add_argument("--calculation-year", type=int, required=True)

    args, unknown_args = p.parse_known_args(args=command_line_args)
    if len(unknown_args):
        unknown_args_text = ", ".join(unknown_args)
        raise Exception(f"Unknown args: {unknown_args_text}")

    return args
