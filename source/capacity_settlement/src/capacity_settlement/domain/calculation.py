from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
    FloatType,
)
from telemetry_logging import use_span

from source.capacity_settlement.src.capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)
from source.capacity_settlement.src.capacity_settlement.infrastructure.spark_initializor import (
    initialize_spark,
)


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

    metering_point_periods = metering_point_periods.withColumn(
        "selection_period_start", F.lit(calculation_period_start)
    ).withColumn("selection_period_end", F.lit(calculation_period_end))

    metering_point_time_series = time_series_points.join(
        metering_point_periods, on="metering_point_id", how="inner"
    ).where(
        (F.col("observation_time") >= F.col("selection_period_start"))
        & (F.col("observation_time") <= F.col("selection_period_end"))
    )

    # average the quantity for each metering point
    metering_point_time_series = metering_point_time_series.groupBy(
        "metering_point_id", "selection_period_start", "selection_period_end"
    ).agg(F.avg("quantity").alias("average_quantity"))

    # explode between the start and end date
    metering_point_time_series = metering_point_time_series.withColumn(
        "date",
        F.explode(
            F.sequence(
                "selection_period_start",
                "selection_period_end",
                F.expr("interval 1 day"),
            )
        ),
    )

    # TODO JMG: Remove dummy result and implement the core logic
    return _create_dummy_result()


def _create_dummy_result() -> DataFrame:
    spark = initialize_spark()
    # Define schema
    schema = StructType(
        [
            StructField("metering_point_id", StringType(), True),
            StructField("date", TimestampType(), True),
            StructField("quantity", FloatType(), True),
        ]
    )

    # Data
    data = [
        ("170000000000000201", datetime(2025, 12, 31, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 1, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 2, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 3, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 4, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 5, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 6, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 7, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 8, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 9, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 10, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 11, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 12, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 13, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 14, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 15, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 16, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 17, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 18, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 19, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 20, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 21, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 22, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 23, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 24, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 25, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 26, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 27, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 28, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 29, 23, 0, 0), 3.5),
        ("170000000000000201", datetime(2026, 1, 30, 23, 0, 0), 3.5),
    ]

    return spark.createDataFrame(data, schema)
