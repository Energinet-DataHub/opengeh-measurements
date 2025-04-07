## steps:
# 1. for every metering point find ended periods partially or fully within the last 3 years
# 2. sum over each period and devide it by the number of days in the period
# 3. check if any cnc in ts differs from the newly calculated
# 4. if so, update the cenc


from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.net_consumption_group_6.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)
from geh_calculated_measurements.net_consumption_group_6.domain.cenc_yearly import (
    _filter_relevant_time_series_points,
    _get_net_consumption_metering_points,
    _join_parent_child_with_consumption,
)


@use_span()
@testing()
def calculate_cenc(
    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
    child_metering_points: ChildMeteringPoints,
    current_measurements: CurrentMeasurements,
    time_zone: str,
    execution_start_datetime: datetime,
) -> CurrentMeasurements:
    # Filter and join the data
    filtered_time_series = _filter_relevant_time_series_points(current_measurements)
    parent_child_joined = _join_parent_child_with_consumption(
        child_metering_points, consumption_metering_point_periods, execution_start_datetime
    )
    # Extract net consumption points
    net_consumption_metering_points = _get_net_consumption_metering_points(parent_child_joined)

    # enforce 3 year
    net_consumption_metering_points = (
        net_consumption_metering_points.select(
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
            F.col(ContractColumnNames.period_to_date).isNull() & F.col(ContractColumnNames.period_to_date)
            <= F.col("cut_off_date")
        )
    )

    # split periods by settelement month
    periods = net_consumption_metering_points.select(
        "*",
        F.when(
            F.month(ContractColumnNames.period_from_date) >= F.col("settlement_month"),
            F.make_date(
                F.date_add(F.col(ContractColumnNames.period_from_date), 1),
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
    ).select(
        "*",
        F.explode(
            F.array(
                F.col(ContractColumnNames.period_from_date),
                F.sequence(
                    F.col("next_settlement_date"),
                    F.col(ContractColumnNames.period_to_date),
                    F.expr("INTERVAL 1 YEAR"),
                ),
            )
        ).alias("period_start"),
        F.explode(
            F.array(
                F.sequence(
                    F.col("next_settlement_date"),
                    F.col(ContractColumnNames.period_to_date),
                    F.expr("INTERVAL 1 YEAR"),
                ),
                F.when(
                    F.month(F.col(ContractColumnNames.period_to_date)) != F.col("settlement_month"),
                    F.col(ContractColumnNames.period_to_date),
                ),
            ),
        ).alias("period_end"),
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

    # ts for each period
    periods_with_ts = periods.alias("mp").join(
        filtered_time_series.alias("ts"),
        on=[
            F.col(f"mp.{ContractColumnNames.metering_point_id}")
            == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
            (F.col(f"ts.{ContractColumnNames.observation_time}") >= F.col("ts.period_start"))
            & (F.col(f"ts.{ContractColumnNames.observation_time}") <= F.col("ts.period_end")),
        ],
        how="left",
    )

    # sum ts over each period
    net_consumption_over_ts = (
        periods_with_ts.groupBy(
            F.col(ContractColumnNames.metering_point_id),
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
    # calculate daily quantity
    periods_w_net_consumption = periods_w_cut_off.join(
        net_consumption_over_ts,
        on=[
            F.col(ContractColumnNames.metering_point_id),
            F.col("period_start"),
            F.col("period_end"),
        ],
        how="inner",
    ).select(
        "*",
        F.when(
            F.col("net_consumption") < 0,
            F.lit(0),
        )
        .otherwise(
            F.col("net_consumption")
            / F.datediff(F.col(ContractColumnNames.period_from_date), F.col(ContractColumnNames.period_to_date))
        )
        .alias("daily_quantity"),
    )

    cnc_measurements = cnc_periods.select(
        "*",
        F.explode(
            F.sequence(
                F.col("period_from_date_with_cut_off"),
                F.date_add(ContractColumnNames.period_to_date, -1),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias(ContractColumnNames.date),
    )

    # check if any cnc differs from the newly calculated
    cnc_diff = (
        cnc_measurements.alias("cnc")
        .join(
            net_consumption_metering_points_with_ts.alias("ts"),
            on=[
                F.col(f"cnc.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                F.col(f"cnc.{ContractColumnNames.uncoupled_date}") == F.col(f"ts.{ContractColumnNames.uncoupled_date}"),
                F.col(f"cnc.{ContractColumnNames.coupled_date}") == F.col(f"ts.{ContractColumnNames.coupled_date}"),
                F.col("cnc.daily_quantity") == F.col(f"ts.{ContractColumnNames.quantity}"),
            ],
            how="anti_left",
        )
        .select(
            F.col(f"ts.{ContractColumnNames.metering_point_type}").alias(ContractColumnNames.metering_point_type),
            F.col(f"ts.{ContractColumnNames.metering_point_id}").alias(ContractColumnNames.metering_point_id),
            F.col(f"ts.{ContractColumnNames.observation_time}").alias(ContractColumnNames.date),
            F.col("cnc.daily_quantity").alias(ContractColumnNames.quantity),
            F.lit("calculated").alias(ContractColumnNames.quality),  # is this correct?
        )
    )

    return CurrentMeasurements(cnc_diff)
