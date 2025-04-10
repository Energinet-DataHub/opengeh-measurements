from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from geh_calculated_measurements.missing_measurements_log.domain import MissingMeasurementsLogTable


@use_span()
def execute_application(spark: SparkSession, args: MissingMeasurementsLogArgs) -> None:
    missing_measurements_log_table = MissingMeasurementsLogTable(
        args.catalog_name, args.time_zone, args.orchestration_instance_id
    )
    # option 1
    df = missing_measurements_log_table.read()
    missing_measurements_log_table.write(df)

    # option 2
    missing_measurements_log_table.write()
