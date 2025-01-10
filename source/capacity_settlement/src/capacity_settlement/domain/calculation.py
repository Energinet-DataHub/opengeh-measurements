from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, FloatType
from telemetry_logging import use_span

from source.capacity_settlement.src.capacity_settlement.application.job_args.capacity_settlement_args import CapacitySettlementArgs
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
    time_zone: str,
) -> DataFrame:
    # TODO JMG: Remove dummy result and implement the core logic
    return _create_dummy_result()


def _create_dummy_result() -> DataFrame:
    spark = initialize_spark()
    # Define schema
    schema = StructType([
        StructField("metering_point_id", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("quantity", FloatType(), True)
    ])

    # Data
    data = [
        ("170000000000000201", "2025-12-31 23:00:00", 3.5),
        ("170000000000000201", "2026-01-01 23:00:00", 3.5),
        ("170000000000000201", "2026-01-02 23:00:00", 3.5),
        ("170000000000000201", "2026-01-03 23:00:00", 3.5),
        ("170000000000000201", "2026-01-04 23:00:00", 3.5),
        ("170000000000000201", "2026-01-05 23:00:00", 3.5),
        ("170000000000000201", "2026-01-06 23:00:00", 3.5),
        ("170000000000000201", "2026-01-07 23:00:00", 3.5),
        ("170000000000000201", "2026-01-08 23:00:00", 3.5),
        ("170000000000000201", "2026-01-09 23:00:00", 3.5),
        ("170000000000000201", "2026-01-10 23:00:00", 3.5),
        ("170000000000000201", "2026-01-11 23:00:00", 3.5),
        ("170000000000000201", "2026-01-12 23:00:00", 3.5),
        ("170000000000000201", "2026-01-13 23:00:00", 3.5),
        ("170000000000000201", "2026-01-14 23:00:00", 3.5),
        ("170000000000000201", "2026-01-15 23:00:00", 3.5),
        ("170000000000000201", "2026-01-16 23:00:00", 3.5),
        ("170000000000000201", "2026-01-17 23:00:00", 3.5),
        ("170000000000000201", "2026-01-18 23:00:00", 3.5),
        ("170000000000000201", "2026-01-19 23:00:00", 3.5),
        ("170000000000000201", "2026-01-20 23:00:00", 3.5),
        ("170000000000000201", "2026-01-21 23:00:00", 3.5),
        ("170000000000000201", "2026-01-22 23:00:00", 3.5),
        ("170000000000000201", "2026-01-23 23:00:00", 3.5),
        ("170000000000000201", "2026-01-24 23:00:00", 3.5),
        ("170000000000000201", "2026-01-25 23:00:00", 3.5),
        ("170000000000000201", "2026-01-26 23:00:00", 3.5),
        ("170000000000000201", "2026-01-27 23:00:00", 3.5),
        ("170000000000000201", "2026-01-28 23:00:00", 3.5),
        ("170000000000000201", "2026-01-29 23:00:00", 3.5),
        ("170000000000000201", "2026-01-30 23:00:00", 3.5)
    ]

    return spark.createDataFrame(data, schema)
