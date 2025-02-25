import sys
import uuid
from argparse import Namespace
from collections.abc import Callable

import geh_common.telemetry.logging_configuration as config
from geh_common.telemetry.decorators import use_span
from geh_common.telemetry.span_recording import span_record_exception
from opentelemetry.trace import SpanKind
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.application import (
    CapacitySettlementArgs,
)
from geh_calculated_measurements.capacity_settlement.application.job_args.command_line_args import (
    parse_command_line_arguments,
    parse_job_arguments,
)
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.capacity_settlement.infrastructure import MeasurementsRepository
from geh_calculated_measurements.capacity_settlement.infrastructure.spark_initializor import (
    initialize_spark,  # put in shared location
)


def execute_application(
    *,
    cloud_role_name: str = "dbr-capacity-settlement",
    applicationinsights_connection_string: str | None = None,
    parse_command_line_args: Callable[..., Namespace] = parse_command_line_arguments,
    parse_job_args: Callable[..., CapacitySettlementArgs] = parse_job_arguments,
) -> None:
    """Start overload with explicit dependencies for easier testing."""
    config.configure_logging(
        cloud_role_name=cloud_role_name,
        tracer_name="capacity-settlement-job",
        applicationinsights_connection_string=applicationinsights_connection_string,
        extras={"Subsystem": "measurements"},
    )

    with config.get_tracer().start_as_current_span(__name__, kind=SpanKind.SERVER) as span:
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
            _execute_application(spark, args)

            @use_span()
            def foo():
                pass

            foo()

        # Added as ConfigArgParse uses sys.exit() rather than raising exceptions
        except SystemExit as e:
            if e.code != 0:
                span_record_exception(e, span)
            sys.exit(e.code)

        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _execute_application(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # Create repositories to obtain data frames
    measurements_repository = MeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_repository.read_time_series_points()
    metering_point_periods = measurements_repository.read_metering_point_periods()

    orchestration_instance_id = uuid.UUID()

    # Execute the domain logic
    execute(
        time_series_points,
        metering_point_periods,
        orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )
