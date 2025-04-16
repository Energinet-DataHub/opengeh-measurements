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
    # convert to local time
    periods_with_net_consumption = convert_from_utc(periods_with_net_consumption, time_zone)
    periods_with_ts = convert_from_utc(periods_with_ts, time_zone)

    # generate daily observations quantity
    cnc_measurements = _expand_periods_to_daily_observations(periods_with_net_consumption)

    # check if any cnc differs from the newly calculated
    cnc_diff = _get_cnc_discrepancies(periods_with_ts, cnc_measurements)

    cnc_diff_utc = convert_to_utc(cnc_diff, time_zone)

    return CalculatedMeasurementsDaily(cnc_diff_utc)


def _get_cnc_discrepancies(periods_with_ts, cnc_measurements) -> DataFrame:
    """Get discrepancies between the calculated and the original CNC measurements.

    This function checks for discrepancies between the calculated CNC measurements and the original CNC measurements.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - date: Daily observation time
            - quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    cnc_diff = (
        cnc_measurements.alias("cnc")
        .join(
            periods_with_ts.alias("ts"),
            on=[
                F.col(f"cnc.{ContractColumnNames.metering_point_id}")
                == F.col(f"ts.{ContractColumnNames.metering_point_id}"),
                F.col(f"cnc.{ContractColumnNames.date}") == F.col(f"ts.{ContractColumnNames.date}"),
            ],
            how="left_anti",
        )
        .select(
            F.col(ContractColumnNames.metering_point_id),
            F.col(f"cnc.{ContractColumnNames.date}").alias(ContractColumnNames.date),
            F.col("cnc.daily_quantity").alias(ContractColumnNames.quantity),
        )
    )

    return cnc_diff


def _expand_periods_to_daily_observations(periods_with_net_consumption) -> DataFrame:
    """Expand periods with net consumption to daily observations.

    This function expands the periods with net consumption into daily observations.

    Returns:
        DataFrame with columns:
            - metering_point_id: ID of the metering point
            - date: Daily observation time
            - daily_quantity: The calculated daily quantity as DecimalType(18, 3)
    """
    cnc_measurements = periods_with_net_consumption.select(
        "*",
        F.explode(
            F.sequence(
                F.col("period_start_with_cut_off"),
                F.date_add(ContractColumnNames.period_to_date, -1),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias(ContractColumnNames.date),
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
        F.col("daily_quantity").cast(T.DecimalType(18, 3)),
    )

    return cnc_measurements
