from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import Window

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cenc import (
    _filter_relevant_time_series_points,
)


@use_span()
@testing()
def execute(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    current_measurements: CurrentMeasurements,
    time_zone: str,
    execution_start_datetime: datetime,
) -> CurrentMeasurements:
    # Filter and join the data
    filtered_time_series = _filter_relevant_time_series_points(current_measurements)
    parent_child_joined = (
        child_metering_points.df.alias("child")
        .join(
            consumption_metering_point_periods.df.alias("consumption"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}")
            == F.col(f"consumption.{ContractColumnNames.metering_point_id}"),
            "left",
        )
        .select(
            F.col(f"child.{ContractColumnNames.metering_point_id}"),
            F.col(f"child.{ContractColumnNames.metering_point_type}"),
            F.col(f"child.{ContractColumnNames.parent_metering_point_id}"),
            F.col(f"consumption.{ContractColumnNames.period_from_date}"),
            F.col(f"consumption.{ContractColumnNames.period_to_date}"),
            F.col(f"consumption.{ContractColumnNames.has_electrical_heating}"),
            F.col(f"consumption.{ContractColumnNames.settlement_month}"),
            F.col("consumption.move_in"),
        )
    )
    print("parent_child_joined")
    parent_child_joined.printSchema()
    parent_child_joined.show()

    # enforce 3 year
    metering_points = (
        parent_child_joined.select(
            "*",
            F.lit(execution_start_datetime).cast(T.TimestampType()).alias("execution_start_datetime"),
        )
        .select(
            "*",
            F.add_months(
                F.make_date(
                    F.year(F.col("execution_start_datetime")),
                    F.month(F.col("execution_start_datetime")),
                    F.day(F.col("execution_start_datetime")),
                ),
                -3 * 12,
            ).alias("cut_off_date"),
        )
        .where(
            F.col(ContractColumnNames.period_to_date).isNotNull()
            | (F.col(ContractColumnNames.period_to_date) <= F.col("cut_off_date"))
        )
    )
    print("metering_points after cut off")
    metering_points.printSchema()
    metering_points.show()

    # split periods by settelement month
    periods = (
        metering_points.select(
            "*",
            F.when(
                F.month(ContractColumnNames.period_from_date) >= F.col("settlement_month"),
                F.make_date(
                    F.year(F.col(ContractColumnNames.period_from_date)) + F.lit(1),
                    F.col("settlement_month"),
                    F.lit(1),
                ),
            )
            .otherwise(
                F.make_date(
                    F.year(F.col(ContractColumnNames.period_from_date)),
                    F.col("settlement_month"),
                    F.lit(1),
                )
            )
            .alias("next_settlement_date"),
        )
        .select(
            "*",
            F.posexplode(
                F.concat(
                    F.array(F.col(ContractColumnNames.period_from_date)),
                    F.sequence(
                        F.col("next_settlement_date"),
                        F.col(ContractColumnNames.period_to_date),
                        F.expr("INTERVAL 1 YEAR"),
                    ),
                    F.array(F.col(ContractColumnNames.period_to_date)),
                )
            ).alias("index", "period_start"),
        )
        .select(
            "*",
            F.lead("period_start", 1)
            .over(
                Window.partitionBy(
                    ContractColumnNames.metering_point_id,
                    ContractColumnNames.period_to_date,
                    ContractColumnNames.period_from_date,
                ).orderBy("index")
            )
            .alias("period_end"),
        )
        .where(F.datediff(F.col("period_end"), F.col("period_start")) > 1)
    )

    # filter out periods that are not in the last 3 years
    periods_w_cut_off = periods.select(
        "*",
        F.when(
            F.col("period_start") < F.col("cut_off_date"),
            F.col("cut_off_date"),
        )
        .otherwise(F.col("period_start"))
        .alias("period_start_with_cut_off"),
    )
    print("periods_w_cut_off")
    periods_w_cut_off.printSchema()
    periods_w_cut_off.show()

    print("filtered_time_series")
    filtered_time_series.printSchema()
    filtered_time_series.show()

    # ts for each period
    periods_w_ts = (
        periods.alias("mp")
        .join(
            filtered_time_series.alias("ts"),
            on=[
                F.col(f"mp.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                (F.col(f"ts.{ContractColumnNames.observation_time}") >= F.col("mp.period_start"))
                & (F.col(f"ts.{ContractColumnNames.observation_time}") <= F.col("mp.period_end")),
            ],
            how="left",
        )
        .select(
            "mp.metering_point_id",
            "mp.metering_point_type",
            "parent_metering_point_id",
            "period_from_date",
            "period_to_date",
            "has_electrical_heating",
            "settlement_month",
            "move_in",
            "execution_start_datetime",
            "cut_off_date",
            "next_settlement_date",
            "period_start",
            "period_end",
            "observation_time",
            "quantity",
            "quality",
        )
    )
    print("periods_with_ts")
    periods_w_ts.printSchema()
    periods_w_ts.show()

    # sum ts over each period
    net_consumption_over_ts = (
        periods_w_ts.groupBy(
            F.col(ContractColumnNames.parent_metering_point_id),
            F.col("period_start"),
            F.col("period_end"),
        )
        .agg(
            F.sum(
                F.when(
                    F.col(ContractColumnNames.metering_point_type) == MeteringPointType.CONSUMPTION_FROM_GRID.value,
                    F.col(ContractColumnNames.quantity),
                ).otherwise(0)
            ).alias(MeteringPointType.CONSUMPTION_FROM_GRID.value),
            F.sum(
                F.when(
                    F.col(ContractColumnNames.metering_point_type) == MeteringPointType.SUPPLY_TO_GRID.value,
                    F.col(ContractColumnNames.quantity),
                ).otherwise(0)
            ).alias(MeteringPointType.SUPPLY_TO_GRID.value),
        )
        .select(
            "*",
            (
                F.col(str(MeteringPointType.CONSUMPTION_FROM_GRID.value))
                - F.col(str(MeteringPointType.SUPPLY_TO_GRID.value))
            ).alias("net_consumption"),
        )
    )
    print("net_consumption_over_ts")
    net_consumption_over_ts.printSchema()
    net_consumption_over_ts.show()
    # calculate daily quantity
    periods_w_net_consumption = (
        periods_w_cut_off.alias("mp")
        .join(
            net_consumption_over_ts.alias("ts"),
            on=[
                F.col(f"mp.{ContractColumnNames.parent_metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.parent_metering_point_id}"),
                F.col("mp.period_start") == F.col("ts.period_start"),
                F.col("mp.period_end") == F.col("ts.period_end"),
                F.col(f"mp.{ContractColumnNames.metering_point_type}") == MeteringPointType.NET_CONSUMPTION.value,
            ],
            how="inner",
        )
        .select(
            "*",
            F.when(
                F.col("net_consumption") < 0,
                F.lit(0),
            )
            .otherwise(
                F.col("net_consumption")
                / F.datediff(F.col(ContractColumnNames.period_to_date), F.col(ContractColumnNames.period_from_date))
            )
            .cast(T.DecimalType(18, 3))
            .alias("daily_quantity"),
        )
        .select(
            "mp.metering_point_id",
            "metering_point_type",
            "mp.parent_metering_point_id",
            "period_from_date",
            "period_to_date",
            "has_electrical_heating",
            "settlement_month",
            "move_in",
            "execution_start_datetime",
            "cut_off_date",
            "next_settlement_date",
            "mp.period_start",
            "mp.period_end",
            "period_start_with_cut_off",
            "consumption_from_grid",
            "supply_to_grid",
            "net_consumption",
            "daily_quantity",
        )
    )
    print("periods_w_net_consumption")
    periods_w_net_consumption.printSchema()
    periods_w_net_consumption.show()

    cnc_measurements = periods_w_net_consumption.select(
        "*",
        F.explode(
            F.sequence(
                F.col("period_start_with_cut_off"),
                F.date_add(ContractColumnNames.period_to_date, -1),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias(ContractColumnNames.date),
    )
    print("cnc_measurements")
    cnc_measurements.printSchema()
    cnc_measurements.show()

    # check if any cnc differs from the newly calculated
    cnc_diff = (
        cnc_measurements.alias("cnc")
        .join(
            periods_w_ts.alias("ts"),
            on=[
                F.col(f"cnc.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                F.col(f"cnc.{ContractColumnNames.date}") == F.col(f"ts.{ContractColumnNames.observation_time}"),
            ],
            how="left_anti",
        )
        .select(
            F.col(ContractColumnNames.metering_point_type),
            F.col(ContractColumnNames.metering_point_id),
            F.col(f"cnc.{ContractColumnNames.date}").alias(ContractColumnNames.observation_time),
            F.col("cnc.daily_quantity").alias(ContractColumnNames.quantity),
            F.lit("calculated").alias(ContractColumnNames.quality),  # is this correct?
            F.lit("00000000-0000-0000-0000-000000000001").alias(ContractColumnNames.orchestration_instance_id),
            F.lit("[IGNORE]").alias(ContractColumnNames.transaction_id),
            F.lit("2020-01-01 00:00:00").alias(ContractColumnNames.transaction_creation_datetime),
        )
    )
    print("cnc_diff")
    cnc_diff.printSchema()
    cnc_diff.show()

    return CurrentMeasurements(cnc_diff)
