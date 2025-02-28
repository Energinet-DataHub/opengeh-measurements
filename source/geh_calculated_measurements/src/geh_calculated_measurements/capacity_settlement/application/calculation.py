
from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.capacity_settlement.infrastructure import MeasurementsRepository


@use_span()
def execute_application(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # Create repositories to obtain data frames
    measurements_repository = MeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_repository.read_time_series_points()
    metering_point_periods = measurements_repository.read_metering_point_periods()

    # Execute the domain logic
    execute(
        time_series_points,
        metering_point_periods,
        args.orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )
