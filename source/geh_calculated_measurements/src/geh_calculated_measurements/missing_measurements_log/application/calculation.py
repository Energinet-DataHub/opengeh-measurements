from geh_common.telemetry.decorators import use_span
from pyspark.sql import DataFrame, SparkSession

from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from geh_calculated_measurements.missing_measurements_log.domain import MissingMeasurementsLogTable


@use_span()
def execute_application(spark: SparkSession, args: MissingMeasurementsLogArgs) -> DataFrame:
    missing_measurements_log_table = MissingMeasurementsLogTable(
        args.catalog_name,
        args.time_zone,
        args.orchestration_instance_id,
        args.grid_area_codes,
        args.period_start_datetime,
        args.period_end_datetime,
    )
    df = missing_measurements_log_table.read()
    # TODO JMG: missing_measurements_log_table.write(df)
    return df
