from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsRepository
from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)
from geh_calculated_measurements.net_consumption_group_6.domain import execute


@use_span()
def execute_application(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.electricity_market_data_path)
    measurements_gold_repository = MeasurementsGoldRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_gold_repository.read_time_series_points()
    consumption_metering_point_periods = electricity_market_repository.read_consumption_metering_point_periods()
    child_metering_point_periods = electricity_market_repository.read_child_metering_points()

    _, calculated_measurements = execute(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
        args.orchestration_instance_id,
        args.execution_start_datetime,
    )

    # Write the calculated measurements
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements)
