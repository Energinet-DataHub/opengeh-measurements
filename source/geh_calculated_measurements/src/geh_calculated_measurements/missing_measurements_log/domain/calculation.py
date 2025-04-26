from datetime import datetime
from uuid import UUID

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.application import GridAreaCodes
from geh_common.domain.types import MeteringPointResolution
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from geh_common.telemetry import use_span
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames, CurrentMeasurements
from geh_calculated_measurements.missing_measurements_log.domain.clamp import clamp_period
from geh_calculated_measurements.missing_measurements_log.domain.model.metering_point_periods import (
    MeteringPointPeriods,
)


@use_span()
def execute(
    current_measurements: CurrentMeasurements,
    metering_point_periods: MeteringPointPeriods,
    grid_area_codes: GridAreaCodes | None,
    orchestration_instance_id: UUID,
    time_zone: str,
    period_start_datetime: datetime,
    period_end_datetime: datetime,
) -> DataFrame:
    metering_point_periods_df = metering_point_periods.df
    if grid_area_codes is not None:
        metering_point_periods_df = metering_point_periods_df.where(
            F.col(ContractColumnNames.grid_area_code).isin(grid_area_codes)
        )

    expected_measurement_counts = _get_expected_measurement_counts(
        metering_point_periods_df, time_zone, period_start_datetime, period_end_datetime
    )
    actual_measurement_counts = _get_actual_measurement_counts(current_measurements, time_zone)

    missing_measurements = _get_missing_measurements(
        expected_measurement_counts=expected_measurement_counts,
        actual_measurement_counts=actual_measurement_counts,
    )

    return missing_measurements.withColumn(
        ContractColumnNames.orchestration_instance_id, F.lit(str(orchestration_instance_id))
    )


def _get_expected_measurement_counts(
    metering_point_periods: DataFrame, time_zone: str, period_start_datetime: datetime, period_end_datetime: datetime
) -> DataFrame:
    """Calculate the expected measurement counts grouped by metering point and date."""
    metering_point_periods = clamp_period(
        metering_point_periods,
        period_start_datetime,
        period_end_datetime,
        ContractColumnNames.period_from_date,
        ContractColumnNames.period_to_date,
    )

    metering_point_periods_local_time = convert_from_utc(metering_point_periods, time_zone)

    metering_point_periods_daily = (
        metering_point_periods_local_time.withColumn(
            "start_of_day",
            F.explode(
                F.sequence(
                    F.col(ContractColumnNames.period_from_date),
                    F.col(ContractColumnNames.period_to_date),
                    F.expr("INTERVAL 1 DAY"),
                )
            ),
        )
        .where(
            # to date is exclusive
            F.col("start_of_day") < F.col(ContractColumnNames.period_to_date)
        )
        .withColumn("end_of_day", F.col("start_of_day") + F.expr("INTERVAL 1 DAY"))
    )

    metering_point_periods_daily = convert_to_utc(metering_point_periods_daily, time_zone)

    expected_measurement_counts = metering_point_periods_daily.select(
        F.col(ContractColumnNames.metering_point_id),
        F.col("start_of_day"),
        (
            (F.unix_timestamp(F.col("end_of_day")) - F.unix_timestamp(F.col("start_of_day")))
            / F.when(F.col(ContractColumnNames.resolution) == MeteringPointResolution.HOUR.value, 3600).when(
                F.col(ContractColumnNames.resolution) == MeteringPointResolution.QUARTER.value, 900
            )
            # 3600 seconds for 1 hour, 900 seconds for 15 minutes
        ).alias("measurement_counts"),
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col("start_of_day").alias(ContractColumnNames.date),
        F.col("measurement_counts"),
    )

    return expected_measurement_counts


def _get_actual_measurement_counts(current_measurements: CurrentMeasurements, time_zone: str) -> DataFrame:
    """Calculate the actual measurement counts grouped by metering point and date."""
    current_measurements_df = current_measurements.df.where(
        F.col(ContractColumnNames.observation_time).isNotNull()
    ).where(F.col(ContractColumnNames.quality) != "missing")  # TODO Use data type from common.

    current_measurements_df = convert_from_utc(current_measurements_df, time_zone)

    actual_measurement_counts = (
        current_measurements_df.withColumn(
            ContractColumnNames.date, F.to_date(F.col(ContractColumnNames.observation_time))
        )
        .groupBy(ContractColumnNames.date, ContractColumnNames.metering_point_id)
        .agg(F.count("*").alias("measurement_counts"))
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date).cast(T.TimestampType()),
        F.col("measurement_counts"),
    )

    return convert_to_utc(actual_measurement_counts, time_zone)


def _get_missing_measurements(
    expected_measurement_counts: DataFrame, actual_measurement_counts: DataFrame
) -> DataFrame:
    return expected_measurement_counts.join(
        actual_measurement_counts,
        [
            expected_measurement_counts[ContractColumnNames.metering_point_id]
            == actual_measurement_counts[ContractColumnNames.metering_point_id],
            expected_measurement_counts[ContractColumnNames.date]
            == actual_measurement_counts[ContractColumnNames.date],
            expected_measurement_counts["measurement_counts"] == actual_measurement_counts["measurement_counts"],
        ],
        "left_anti",
    ).select(
        F.col(ContractColumnNames.metering_point_id),
        F.col(ContractColumnNames.date),
    )
