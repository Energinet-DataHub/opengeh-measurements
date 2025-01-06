import sys
import uuid
from argparse import Namespace
from collections.abc import Callable

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from pyspark._typing import F
from pyspark.sql import SparkSession
from telemetry_logging import use_span
from telemetry_logging.span_recording import span_record_exception

import source.electrical_heating.src.electrical_heating.infrastructure.electrical_heating_internal as ehi
import source.electrical_heating.src.electrical_heating.infrastructure.electricity_market as em
import source.electrical_heating.src.electrical_heating.infrastructure.measurements_gold as mg
from source.electrical_heating.src.electrical_heating.application.job_args.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from source.electrical_heating.src.electrical_heating.application.job_args.electrical_heating_job_args import (
    parse_command_line_arguments,
    parse_job_arguments,
)
from source.electrical_heating.src.electrical_heating.domain.calculation import (
    execute_core_logic,
)
from source.electrical_heating.src.electrical_heating.domain.calculation_results import (
    CalculationOutput,
)
from source.electrical_heating.src.electrical_heating.infrastructure.spark_initializor import (
    initialize_spark,
)


def execute_with_deps(
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
            _execute_with_deps(spark, args)

        # Added as ConfigArgParse uses sys.exit() rather than raising exceptions
        except SystemExit as e:
            if e.code != 0:
                span_record_exception(e, span)
            sys.exit(e.code)

        except Exception as e:
            span_record_exception(e, span)
            sys.exit(4)


@use_span()
def _execute_with_deps(
    spark: SparkSession, args: ElectricalHeatingArgs
) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = em.Repository(spark, args.catalog_name)
    measurements_gold_repository = mg.Repository(spark, args.catalog_name)
    electrical_heating_internal_repository = ehi.Repository(spark, args.catalog_name)

    calculation_output = CalculationOutput()

    # Temp. calculation id
    calculation_id = str(uuid.uuid4())

    # Read data frames
    consumption_metering_point_periods = (
        electricity_market_repository.read_consumption_metering_point_periods()
    )
    child_metering_point_periods = (
        electricity_market_repository.read_child_metering_point_periods()
    )
    time_series_points = measurements_gold_repository.read_time_series_points()

    _execute(
        args,
        calculation_id,
        calculation_output,
        child_metering_point_periods,
        consumption_metering_point_periods,
        electrical_heating_internal_repository,
        time_series_points,
    )


def _execute(
    args,
    calculation_id,
    calculation_output,
    child_metering_point_periods,
    consumption_metering_point_periods,
    electrical_heating_internal_repository,
    time_series_points,
) -> CalculationOutput:
    # Execute the calculation logic and store it.
    calculation_output.daily_child_consumption_with_limit = execute_core_logic(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )
    # Find the calculation metadata and store it.
    calculation_output.calculations = (
        electrical_heating_internal_repository.read_calculations().where(
            F.col("calculation_id") == calculation_id
        )
    )

    return calculation_output
