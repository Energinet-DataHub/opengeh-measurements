from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.measurements_report.application.measurements_report_args import MeasurementsReportArgs
from geh_calculated_measurements.measurements_report.domain import (
    execute,
)


@use_span()
def execute_application(spark: SparkSession, args: MeasurementsReportArgs) -> None:
    execute()
