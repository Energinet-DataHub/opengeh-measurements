import sys
from argparse import Namespace
from collections.abc import Callable

import telemetry_logging.logging_configuration as config
from opentelemetry.trace import SpanKind
from pyspark.sql import SparkSession
from telemetry_logging import use_span

from opengeh_electrical_heating.domain import (
    execute,
)
from opengeh_electrical_heating.infrastructure import (
    ElectricityMarketRepository,
    MeasurementsRepository,
)


@use_span()
def execute_application(spark: SparkSession, args: ElectricalHeatingArgs) -> None:
    execution_start_datetime = datetime.now(timezone.utc)

    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.electricity_market_data_path)
    measurements_repository = MeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_repository.read_time_series_points()
    consumption_metering_point_periods = electricity_market_repository.read_consumption_metering_point_periods()
    child_metering_point_periods = electricity_market_repository.read_child_metering_points()

    # Execute the domain logic
    execute(
        spark,
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )
