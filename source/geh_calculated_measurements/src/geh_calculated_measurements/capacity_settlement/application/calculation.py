import uuid

from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsRepository


@use_span()
def execute_application(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # Create repositories to obtain data frames
    measurements_repository = MeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_repository.read_time_series_points()
    metering_point_periods = measurements_repository.read_metering_point_periods()

    orchestration_instance_id = uuid.UUID()

    # Execute the domain logic
    calculated_measurements = execute(
        time_series_points,
        metering_point_periods,
        orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )

    # Write the calculated measurements
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements)
