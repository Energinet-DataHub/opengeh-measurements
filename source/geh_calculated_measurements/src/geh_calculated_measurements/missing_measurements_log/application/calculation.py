from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import (
    CurrentMeasurementsRepository,
)
from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from geh_calculated_measurements.missing_measurements_log.domain import (
    execute,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure import (
    Repository as MeteringPointPeriodsRepository,
)


@use_span()
def execute_application(spark: SparkSession, args: MissingMeasurementsLogArgs) -> None:
    # Create repositories to obtain data frames
    metering_point_periods_repository = MeteringPointPeriodsRepository(spark, args.catalog_name)
    measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    metering_point_periods = metering_point_periods_repository.read_metering_point_periods()
    measurements = measurements_repository.read_current_measurements()
    execute(metering_point_periods, measurements)
