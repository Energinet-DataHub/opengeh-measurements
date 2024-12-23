import os
import sys
from argparse import Namespace
from collections.abc import Callable

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from telemetry_logging.span_recording import span_record_exception

from source.electrical_heating.src.electrical_heating.domain import calculation


from source.electrical_heating.src.electrical_heating.entry_points.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from source.electrical_heating.src.electrical_heating.entry_points.job_args.electrical_heating_job_args import (
    parse_command_line_arguments,
    parse_job_arguments,
)
from source.electrical_heating.src.electrical_heating.infrastructure.spark_initializor import initialize_spark


def execute() -> None:
    applicationinsights_connection_string = os.getenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )

    start_with_deps(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )


def start_with_deps(
    *,
    cloud_role_name: str = "dbr-electrical-heating",
    applicationinsights_connection_string: str | None = None,
    parse_command_line_args: Callable[..., Namespace] = parse_command_line_arguments,
    parse_job_args: Callable[..., ElectricalHeatingArgs] = parse_job_arguments,
) -> None:
    """Start overload with explicit dependencies for easier testing."""
    config.configure_logging(
        cloud_role_name=cloud_role_name,
        tracer_name="electrical-heating-job",
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras={"Subsystem": "measurements"},
    )

    with config.get_tracer().start_as_current_span(
        __name__, kind=SpanKind.SERVER
    ) as span:
        # Try/except added to enable adding custom fields to the exception as
        # the span attributes do not appear to be included in the exception.
        try:

            # The command line arguments are parsed to have necessary information for
            # coming log messages
            command_line_args = parse_command_line_args()

            # Add extra to structured logging data to be included in every log message.
            config.add_extras(
                {
                    "orchestration-instance-id": command_line_args.orchestration_instance_id,
                }
            )
            span.set_attributes(config.get_extras())
            args = parse_job_args(command_line_args)
            spark = initialize_spark()
            calculation.execute(spark, args)

        # Added as ConfigArgParse uses sys.exit() rather than raising exceptions
        except SystemExit as e:
            if e.code != 0:
                span_record_exception(e, span)
            sys.exit(e.code)

        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)
