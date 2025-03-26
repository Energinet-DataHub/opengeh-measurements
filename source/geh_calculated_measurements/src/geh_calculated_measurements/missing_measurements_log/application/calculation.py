from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from geh_calculated_measurements.missing_measurements_log.domain import (
    execute,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure import Repository


@use_span()
def execute_application(spark: SparkSession, args: MissingMeasurementsLogArgs) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = Repository(spark, args.catalog_name)

    # Read data frames
    metering_point_periods = electricity_market_repository.read_metering_point_periods()
    execute(metering_point_periods)
