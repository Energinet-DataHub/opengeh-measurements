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
    df = missing_measurements_log_table.read()
    df.show()
    # TODO JMG: missing_measurements_log_table.write(df)


def execute_application_test(spark: SparkSession, args: MissingMeasurementsLogArgs) -> None:
    composite_table = MissingMeasurementsLogTable(args.catalog_name, args.time_zone, args.orchestration_instance_id)
    df = composite_table.read()

    df.show()
    # TODO JMG: missing_measurements_log_table.write(df)
