import sys
from datetime import UTC, datetime, timezone

from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from opengeh_electrical_heating.application.electrical_heating_args import ElectricalHeatingArgs
from opengeh_electrical_heating.domain import (
    execute,
)
from opengeh_electrical_heating.infrastructure import (
    ElectricityMarketRepository,
    MeasurementsRepository,
)


@use_span()
def execute_application(spark: SparkSession, args: ElectricalHeatingArgs) -> None:
    execution_start_datetime = datetime.now(UTC)

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
