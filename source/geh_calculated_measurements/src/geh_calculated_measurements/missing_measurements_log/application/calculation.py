from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CurrentMeasurementsRepository
from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from geh_calculated_measurements.missing_measurements_log.domain import execute
from geh_calculated_measurements.missing_measurements_log.infrastructure import (
    ElectricityMarketRepository,
    MissingMeasurementsLogRepository,
)


@use_span()
def execute_application(spark: SparkSession, args: MissingMeasurementsLogArgs) -> None:
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)
    electricity_market_repository = ElectricityMarketRepository(spark, args.catalog_name)

    # Read data frames
    current_measurements = current_measurements_repository.read_current_measurements()
    metering_point_periods = electricity_market_repository.read_metering_point_periods()

    missing_measurements_log = execute(
        current_measurements=current_measurements,
        metering_point_periods=metering_point_periods,
        time_zone=args.time_zone,
        orchestration_instance_id=args.orchestration_instance_id,
        grid_area_codes=args.grid_area_codes,
        period_start_datetime=args.period_start_datetime,
        period_end_datetime=args.period_end_datetime,
    )

    missing_measurements_log_repository = MissingMeasurementsLogRepository(spark, args.catalog_name)
    missing_measurements_log_repository.write_missing_measurements_log(missing_measurements_log)
