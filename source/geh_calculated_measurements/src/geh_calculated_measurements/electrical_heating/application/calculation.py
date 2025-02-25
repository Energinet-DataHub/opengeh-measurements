import sys
from argparse import Namespace
from collections.abc import Callable

import geh_common.telemetry.logging_configuration as config
from geh_common.telemetry import use_span
from geh_common.telemetry.span_recording import span_record_exception
from opentelemetry.trace import SpanKind
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure.spark_initializor import initialize_spark
from geh_calculated_measurements.electrical_heating.application.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from geh_calculated_measurements.electrical_heating.application.job_args.command_line_args import (
    parse_command_line_arguments,
    parse_job_arguments,
)
from geh_calculated_measurements.electrical_heating.domain import (
    execute,
)
from geh_calculated_measurements.electrical_heating.infrastructure import (
    ElectricityMarketRepository,
    MeasurementsGoldRepository,
)


def execute_application(
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

        # Added as ConfigArgParse uses sys.exit() rather than raising exceptions
        except SystemExit as e:
            if e.code != 0:
                span_record_exception(e, span)
            sys.exit(e.code)

        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _execute_application(spark: SparkSession, args: ElectricalHeatingArgs) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.electricity_market_data_path)
    measurements_gold_repository = MeasurementsGoldRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_gold_repository.read_time_series_points()
    consumption_metering_point_periods = electricity_market_repository.read_consumption_metering_point_periods()
    child_metering_point_periods = electricity_market_repository.read_child_metering_points()

    # Execute the domain logic
    execute(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )
