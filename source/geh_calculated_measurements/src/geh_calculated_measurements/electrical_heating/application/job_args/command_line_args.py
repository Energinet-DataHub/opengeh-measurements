import argparse
import sys
import uuid
from argparse import Namespace
from datetime import datetime

import configargparse
from geh_common.telemetry import Logger, logging_configuration

from geh_calculated_measurements.electrical_heating.application.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from geh_calculated_measurements.electrical_heating.application.job_args.environment_variables import (
    get_time_zone,
)


def parse_command_line_arguments() -> Namespace:
    return _parse_args_or_throw(sys.argv[1:])


def parse_job_arguments(
    job_args: Namespace,
) -> ElectricalHeatingArgs:
    logger = Logger(__name__)
    logger.info(f"Command line arguments: {repr(job_args)}")

    with logging_configuration.start_span("electrical_heating.parse_job_arguments"):
        electrical_heating_args = ElectricalHeatingArgs(
            orchestration_instance_id=uuid.UUID(job_args.orchestration_instance_id),
            time_zone=get_time_zone(),
            # Databricks location
            catalog_name=job_args.catalog_name,
            schema_name=job_args.schema_name,
            # Databricks tables
            time_series_points_table=job_args.time_series_points_table,
            consumption_points_table=job_args.consumption_points_table,
            child_points_table=job_args.child_points_table,
        )

    return electrical_heating_args


def _parse_args_or_throw(command_line_args: list[str]) -> argparse.Namespace:
    p = configargparse.ArgParser(
        description="Execute electrical heating calculation",
        formatter_class=configargparse.ArgumentDefaultsHelpFormatter,
    )

    # Run parameters
    p.add_argument("--orchestration-instance-id", type=str, required=True)

    # Databricks catalog parameters
    p.add_argument("--catalog-name", type=str, required=True, help="Databricks catalog name")
    p.add_argument("--schema-name", type=str, required=True, help="Databricks schema name")

    # Table parameters
    p.add_argument("--time-series-points-table", type=str, required=True, help="Table name for time series points")
    p.add_argument(
        "--consumption-points-table", type=str, required=True, help="Table name for consumption metering points"
    )
    p.add_argument("--child-points-table", type=str, required=True, help="Table name for child metering points")

    args, unknown_args = p.parse_known_args(args=command_line_args)
    if len(unknown_args):
        unknown_args_text = ", ".join(unknown_args)
        raise Exception(f"Unknown args: {unknown_args_text}")

    return args


def valid_date(s: str) -> datetime:
    """See https://stackoverflow.com/questions/25470844/specify-date-format-for-python-argparse-input-arguments."""
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        msg = f"not a valid date: {s!r}"
        raise configargparse.ArgumentTypeError(msg)
