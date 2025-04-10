from datetime import datetime

import pyspark.sql.functions as F
from geh_common.domain.types import MeteringPointType
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import Column
from pyspark.sql import types as T

from geh_calculated_measurements.common.domain import (
    ContractColumnNames,
    CurrentMeasurements,
)
from geh_calculated_measurements.common.domain.model.calculated_measurements import CalculatedMeasurementsDaily
from geh_calculated_measurements.net_consumption_group_6.domain import (
    Cenc,
)


def days_in_year(year: Column, month: Column) -> Column:
    # Create date for January 1st of the given year (for Column input)
    start_date = F.make_date(year, month, F.lit(1))

    # Create date for January 1st of the next year
    end_date = F.make_date(year + 1, month, F.lit(1))

    # Calculate the difference in days
    return F.datediff(end_date, start_date)


@use_span()
@testing()
def calculate_daily(
    current_measurements: CurrentMeasurements,
    cenc: Cenc,
    time_zone: str,
    execution_start_datetime: datetime,
) -> CalculatedMeasurementsDaily:
    cenc_added_col = cenc.df.select(  # adding needed columns
        "*",
        F.lit(MeteringPointType.NET_CONSUMPTION.value).alias(ContractColumnNames.metering_point_type),
        F.lit(execution_start_datetime).alias("execution_start_datetime"),
    )

    time_series_points_df = current_measurements.df

    cenc_added_col = convert_from_utc(cenc_added_col, time_zone)

    time_series_points_df = convert_from_utc(time_series_points_df, time_zone)

    cenc_selected_col = cenc_added_col.select(  # selecting needed columns
        F.make_date(F.col("settlement_year"), F.col(ContractColumnNames.settlement_month), F.lit(1)).alias(
            "settlement_date"
        ),
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.metering_point_type),
        (
            F.col(ContractColumnNames.quantity)
            / days_in_year(F.col("settlement_year"), F.col(ContractColumnNames.settlement_month))
        )
        .cast(T.DecimalType(18, 3))
        .alias(ContractColumnNames.quantity),
        F.col("execution_start_datetime"),
    )

    latest_measurements_date = (
        time_series_points_df.where(
            F.col(ContractColumnNames.metering_point_type) == MeteringPointType.NET_CONSUMPTION.value
        )
        .groupBy(ContractColumnNames.metering_point_id)
        .agg(F.max(ContractColumnNames.observation_time).alias("latest_observation_date"))
    )

    # merging the with internal
    cenc_w_last_run = (
        cenc_selected_col.alias("cenc")
        .join(
            latest_measurements_date.alias("ts"),
            on=[ContractColumnNames.metering_point_id],
            how="left",
        )
        .select(
            "cenc.*",
            F.when(
                F.col("ts.latest_observation_date").isNull()
                | (F.col("ts.latest_observation_date") < F.col("cenc.settlement_date")),
                F.date_add(F.col("cenc.settlement_date"), -1),
            )
            .otherwise(F.col("ts.latest_observation_date"))
            .alias("last_run"),
        )
    )

    # Filter out rows where last_run is >= execution_start_datetime
    filtered_cenc = cenc_w_last_run.filter(F.col("last_run") < F.col("execution_start_datetime"))

    # Process only valid date ranges
    df = filtered_cenc.select(
        "*",
        F.explode(
            F.sequence(F.date_add(F.col("last_run"), 1), F.col("execution_start_datetime"), F.expr("INTERVAL 1 DAY"))
        ).alias("date"),
    )

    result_df = df.select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col(ContractColumnNames.quantity),
    )

    result_df = convert_to_utc(result_df, time_zone)

    return CalculatedMeasurementsDaily(result_df)
