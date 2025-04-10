from geh_common.telemetry import use_span
from pyspark.sql import DataFrame


@use_span()
def execute(metering_point_periods: DataFrame, current_measurements: DataFrame) -> None:
    metering_point_periods.show(n=20)  # TODO JMG: Remove this line and do the actual calculation
    current_measurements.show(n=20)
