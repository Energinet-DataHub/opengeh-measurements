from pyspark.sql import DataFrame, SparkSession
from telemetry_logging import use_span

from source.capacity_settlement.src.capacity_settlement.application.job_args.capacity_settlement_args import \
    CapacitySettlementArgs



@use_span()
def execute(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # TODO JMG: read data from repository and call the `execute_core_logic` method
    pass

# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    time_zone: str,
) -> DataFrame:
    # TODO JMG: Implement the core logic of the capacity settlement calculation
    pass

