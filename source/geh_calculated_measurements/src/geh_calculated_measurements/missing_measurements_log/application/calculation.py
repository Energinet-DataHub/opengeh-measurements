from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import (
    CurrentMeasurementsTable,
)
from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from geh_calculated_measurements.missing_measurements_log.domain import (
    execute,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure import MeteringPointPeriodsTable


@use_span()
def execute_application(spark: SparkSession, args: MissingMeasurementsLogArgs) -> None:
    # Create repositories to obtain data frames
    metering_point_periods_table = MeteringPointPeriodsTable(args.catalog_name, args.electricity_market_database_name)
    current_measurements_table = CurrentMeasurementsTable(args.catalog_name)

    # Read data frames
    metering_point_periods = metering_point_periods_table.read()
    measurements = current_measurements_table.read()
    execute(metering_point_periods, measurements)
