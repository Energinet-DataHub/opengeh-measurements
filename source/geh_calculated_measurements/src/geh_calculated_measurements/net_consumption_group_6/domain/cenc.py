from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import initialize_spark
from geh_calculated_measurements.net_consumption_group_6.domain.model import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
    TimeSeriesPoints,
)

_cenc_schema = T.StructType(
    [
        T.StructField("orchestration_instance_id", T.StringType(), False),
        T.StructField("metering_point_id", T.StringType(), False),
        T.StructField("quantity", T.DecimalType(18, 3), False),
        T.StructField("settlement_year", T.IntegerType(), False),
        T.StructField("settlement_month", T.IntegerType(), False),
    ]
)


class Cenc(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(df=df, schema=_cenc_schema, ignore_nullability=True)


@use_span()
@testing()
def calculate_cenc(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    time_series_points: TimeSeriesPoints,
    time_zone: str,
    orchestration_instance_id: str,
    execution_start_datetime: datetime,
) -> Cenc:
    """Return a data frame with schema `cenc_schema`."""
    # D06 = Supply to grid
    # D07 = Consumption from grid
    spark = initialize_spark()
    filtered_time_series_points = time_series_points.df.filter(
        (F.col(ContractColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value)
        | (F.col(ContractColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value)
    )
    filtered_time_series_points.show()
    current_year = datetime.now().year
    parent_and_child_metering_points_joined = (
        child_metering_points.df.alias("child")
        .join(
            consumption_metering_point_periods.df.alias("consumption"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}")
            == F.col(f"consumption.{ContractColumnNames.metering_point_id}"),
            "left",
        )
        .withColumn(
            "settlement_month_timestamp",
            F.to_timestamp(F.concat_ws("-", F.lit(current_year), F.col("settlement_month"), F.lit(1)), "yyyy-M-d"),
        )
        .drop("consumption." + ContractColumnNames.metering_point_id)
    ).select(
        F.col("child." + ContractColumnNames.metering_point_id),
        F.col("child." + ContractColumnNames.metering_point_type),
        F.col("settlement_month_timestamp"),
        F.col("child." + ContractColumnNames.parent_metering_point_id),
    )

    parent_and_child_metering_points_joined.show()
    net_consumption_metering_points = parent_and_child_metering_points_joined.filter(
        F.col(ContractColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value
    )
    net_consumption_metering_points.show()
    joined_ts_mp = (
        parent_and_child_metering_points_joined.join(
            filtered_time_series_points, on=["metering_point_id", "metering_point_type"], how="left"
        )
        .filter(
            F.col("observation_time").between(
                F.add_months(F.col("settlement_month_timestamp"), -12), F.col("settlement_month_timestamp")
            )
        )
        .groupBy("metering_point_id", "metering_point_type", "settlement_month_timestamp", "parent_metering_point_id")
        .agg(F.sum("quantity").alias("quantity"))
    )
    joined_ts_mp.show()
    # Find rows where consumption_from_grid and supply_to_grid have the same parent_metering_point_id and settlement_month_timestamp
    consumption_and_supply = (
        joined_ts_mp.filter(
            (F.col("metering_point_type") == MeteringPointType.CONSUMPTION_FROM_GRID.value)
            | (F.col("metering_point_type") == MeteringPointType.SUPPLY_TO_GRID.value)
        )
        .groupBy("parent_metering_point_id", "settlement_month_timestamp")
        .pivot(
            "metering_point_type",
            [MeteringPointType.CONSUMPTION_FROM_GRID.value, MeteringPointType.SUPPLY_TO_GRID.value],
        )
        .agg(F.sum("quantity").alias("quantity"))
    )

    # Calculate the POS((quantity from consumption_from_grid – quantity from supply_to_grid); 0)
    net_quantity = consumption_and_supply.withColumn(
        "net_quantity",
        F.when(
            F.col(str(MeteringPointType.CONSUMPTION_FROM_GRID.value)).isNotNull()
            & F.col(str(MeteringPointType.SUPPLY_TO_GRID.value)).isNotNull(),
            F.greatest(
                F.col(str(MeteringPointType.CONSUMPTION_FROM_GRID.value))
                - F.col(str(MeteringPointType.SUPPLY_TO_GRID.value)),
                F.lit(0),
            ),
        ).otherwise(F.lit(0)),
    )
    net_quantity.show()

    # Join net_quantity with net_consumption_metering_points on parent_metering_point_id and settlement_month_timestamp
    final_result = (
        net_consumption_metering_points.join(
            net_quantity,
            on=["parent_metering_point_id", "settlement_month_timestamp"],
            how="left",
        )
        .withColumn("orchestration_instance_id", F.lit(orchestration_instance_id))
        .select(
            F.col("orchestration_instance_id"),
            F.col("metering_point_id"),
            F.col("net_quantity").alias("quantity").cast(T.DecimalType(18, 3)),
            F.year(F.col("settlement_month_timestamp")).alias("settlement_year"),
            F.month(F.col("settlement_month_timestamp")).alias("settlement_month"),
        )
    )

    final_result.show()

    return Cenc(final_result)
