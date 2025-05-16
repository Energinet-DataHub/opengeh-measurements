from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from geh_common.testing.dataframes import testing
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily


@use_span()
@testing()
def cnc_daily(
    periods_with_net_consumption: DataFrame,
    periods_with_ts: DataFrame,
    time_zone: str,
    execution_start_datetime: datetime,
) -> CalculatedMeasurementsDaily:
    """Process net consumption data to calculate daily measurements and identify discrepancies.

    This function converts time data to local timezone, expands periods to daily observations,
    identifies discrepancies between existing and newly calculated net consumption values,
    and returns the discrepancies converted back to UTC.

    Parameters:
    periods_with_net_consumption : DataFrame
      DataFrame containing periods with net consumption data.
    periods_with_ts : DataFrame
      DataFrame containing periods with timestamp data.
    time_zone : str
      The time zone to use for local time conversion.

    Returns:
    CalculatedMeasurementsDaily
      Object containing the calculated net consumption discrepancies in UTC.
    """
    periods_with_net_consumption = periods_with_net_consumption.select(
        "*",
        F.lit(execution_start_datetime).alias("execution_start_datetime"),
    )
    periods_with_net_consumption = convert_from_utc(periods_with_net_consumption, time_zone)
    periods_with_ts = convert_from_utc(periods_with_ts, time_zone)

    CNC_WAITINIG_PERIOD_IN_DAYS = 2
    periods_with_net_consumption = periods_with_net_consumption.select(
        "*",
        F.date_add(F.col("period_end"), CNC_WAITINIG_PERIOD_IN_DAYS).alias("calculate_cnc_from"),
    )

    cnc_measurements = _generate_days_in_periods(periods_with_net_consumption)

    cnc_diff = _cnc_diff_and_full_load_newly_closed_periods(periods_with_ts, cnc_measurements)

    cnc_diff_utc = convert_to_utc(cnc_diff, time_zone)

    return CalculatedMeasurementsDaily(cnc_diff_utc)


def _generate_days_in_periods(periods_with_net_consumption: DataFrame) -> DataFrame:
    """Expand periods with net consumption to daily observations.

    This function expands the periods with net consumption into daily observations.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - date: Daily observation time
            - daily_quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    cnc_measurements = (
        periods_with_net_consumption.where(F.col("calculate_cnc_from") <= F.col("execution_start_datetime"))
        .select(
            "*",
            F.explode(
                F.sequence(
                    F.col("period_start_with_cut_off"),
                    F.date_add(F.col("period_end"), -1),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias(ContractColumnNames.date),
        )
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(ContractColumnNames.date),
            F.col("daily_quantity").cast(T.DecimalType(18, 3)),
            F.col("calculate_cnc_from"),
        )
    )

    return cnc_measurements


def _cnc_diff_and_full_load_newly_closed_periods(periods_with_ts: DataFrame, cnc_measurements: DataFrame) -> DataFrame:
    """Get discrepancies between the calculated and the original CNC measurements.

    This function checks for discrepancies between the calculated CNC measurements and the original CNC measurements.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - date: Daily observation time
            - quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    cnc_diff = (
        cnc_measurements.where(F.col("calculate_cnc_from") < F.col("execution_start_datetime"))
        .alias("cnc")
        .join(
            periods_with_ts.alias("ts"),
            on=[
                F.col(f"cnc.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                F.col(f"cnc.{ContractColumnNames.date}") == F.col(f"ts.{ContractColumnNames.date}"),
                F.col("cnc.daily_quantity") == F.col(f"ts.{ContractColumnNames.quantity}"),
            ],
            how="left_anti",
        )
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(f"cnc.{ContractColumnNames.date}").alias(ContractColumnNames.date),
            F.col("cnc.daily_quantity").alias(ContractColumnNames.quantity),
        )
    )

    newly_closed_cnc_period = cnc_measurements.where(
        F.col("calculate_cnc_from") == F.col("execution_start_datetime")
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col("daily_quantity"),
    )

    cnc_diff_and_full_load = cnc_diff.union(newly_closed_cnc_period)

    return cnc_diff_and_full_load
