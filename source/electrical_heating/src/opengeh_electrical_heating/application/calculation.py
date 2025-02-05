"""A module."""

import sys
import uuid
from argparse import Namespace
from collections.abc import Callable
from datetime import datetime, timezone

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from pyspark.sql import DataFrame, SparkSession
from telemetry_logging import use_span
from telemetry_logging.span_recording import span_record_exception

from opengeh_electrical_heating.application.job_args.electrical_heating_job_args import (
    parse_command_line_arguments,
    parse_job_arguments,
)
from opengeh_electrical_heating.domain import (
    CalculationOutput,
    ColumnNames,
    ElectricalHeatingArgs,
    execute,
)
from opengeh_electrical_heating.infrastructure import (
    Calculations,
    ElectricalHeatingInternalRepository,
    ElectricityMarketRepository,
    MeasurementsRepository,
    initialize_spark,
)
from opengeh_electrical_heating.infrastructure.electrical_heating_internal.calculations.schema import (
    calculations,
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
    execution_start_datetime = datetime.now(timezone.utc)

    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.electricity_market_data_path)
    measurements_repository = MeasurementsRepository(spark, args.catalog_name)
    electrical_heating_internal_repository = ElectricalHeatingInternalRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_repository.read_time_series_points()

    consumption_metering_point_periods = electricity_market_repository.read_consumption_metering_point_periods()

    child_metering_point_periods = electricity_market_repository.read_child_metering_points()

    calculation_output = execute_calculation(
        spark,
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args,
        execution_start_datetime,
    )

    electrical_heating_internal_repository.save(Calculations(calculation_output.calculations))


def execute_calculation(
    spark: SparkSession,
    time_series_points: DataFrame,
    consumption_metering_point_periods: DataFrame,
    child_metering_point_periods: DataFrame,
    args: ElectricalHeatingArgs,
    execution_start_datetime: datetime,
) -> CalculationOutput:
    measurements = execute(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )

    calculations = _create_calculation(
        spark,
        args.orchestration_instance_id,
        execution_start_datetime,
        datetime.now(timezone.utc),
    )

    return CalculationOutput(measurements=measurements, calculations=calculations)


def _create_calculation(
    spark: SparkSession,
    orchestration_instance_id: uuid.UUID,
    execution_start_datetime: datetime,
    execution_stop_datetime: datetime,
) -> DataFrame:
    # TODO Temp. calculation id - refac when calculation id is available
    calculation_id = str(uuid.uuid4())

    data = [
        {
            ColumnNames.calculation_id: calculation_id,
            ColumnNames.orchestration_instance_id: str(orchestration_instance_id),
            ColumnNames.execution_start_datetime: execution_start_datetime,
            ColumnNames.execution_stop_datetime: execution_stop_datetime,
        }
    ]

    return spark.createDataFrame(data, calculations)
