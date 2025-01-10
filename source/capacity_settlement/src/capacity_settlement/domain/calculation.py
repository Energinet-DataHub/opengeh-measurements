from datetime import datetime

from pyspark.sql import functions as F, DataFrame, SparkSession, Window
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, FloatType
from telemetry_logging import use_span

from source.capacity_settlement.src.capacity_settlement.application.job_args.capacity_settlement_args import \
    CapacitySettlementArgs
from source.capacity_settlement.src.capacity_settlement.infrastructure.spark_initializor import initialize_spark


@use_span()
def execute(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # TODO JMG: read data from repository and call the `execute_core_logic` method
    pass

# This is also the function that will be tested using the `testcommon.etl` framework.
@use_span()
def execute_core_logic(
    time_series_points: DataFrame,
    metering_point_periods: DataFrame,
    calculation_period_start: datetime,
    calculation_period_end: datetime,
    time_zone: str,
) -> DataFrame:
    # Join the DataFrames on metering_point_id
    metering_point_time_series = metering_point_periods.join(
        time_series_points,
        on="metering_point_id",
        how="inner"
    ).where(
        (F.col("observation_time") >= calculation_period_start)
        & (F.col("observation_time") <= calculation_period_end)
    )

    measurments = metering_point_time_series.groupBy(
        "metering_point_id",
        "selection_period_start",
        "selection_period_end"
    ).agg(
        F.avg("quantity").alias("average_quantity")
    )
