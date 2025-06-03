import uuid
from datetime import datetime
from uuid import UUID

from geh_common.domain.types import MeteringPointResolution, MeteringPointType, OrchestrationType
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from geh_calculated_measurements.common.application.model.calculated_measurements_internal import (
    CalculatedMeasurementsInternal,
)
from geh_calculated_measurements.common.domain.column_names import ContractColumnNames
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily

UUID_NAMESPACE = uuid.UUID("539ba8c3-5d10-4aa9-81d5-632cfce33e18")
""" Define a fixed UUID to use as the namespace for generating UUID v5 values.
This ensures that all UUIDs generated with this namespace and a given name are stable (i.e., the same input always
produces the same output)."""


def create(
    measurements: CalculatedMeasurementsDaily,
    orchestration_instance_id: UUID,
    orchestration_type: OrchestrationType,
    metering_point_type: MeteringPointType,
    time_zone: str,
    transaction_creation_datetime: datetime,
) -> CalculatedMeasurementsInternal:
    df = measurements.df.withColumn(
        ContractColumnNames.transaction_id, _create_transaction_id_column(orchestration_instance_id, time_zone)
    )

    df = _create_transaction_time_column(df)

    df = _explode_to_hour_values(df, time_zone)

    df = _add_storage_columns(
        df, orchestration_instance_id, orchestration_type, metering_point_type, time_zone, transaction_creation_datetime
    )

    return df


def _explode_to_hour_values(measurements: DataFrame, time_zone: str) -> DataFrame:
    df = (
        # Explode the date column to create a row for each hour in the day
        measurements.withColumn(
            ContractColumnNames.observation_time,
            F.explode(
                F.sequence(
                    F.col(ContractColumnNames.date),
                    F.to_utc_timestamp(
                        F.date_add(F.from_utc_timestamp(F.col(ContractColumnNames.date), time_zone), 1)
                        - F.expr("INTERVAL 1 SECOND"),
                        time_zone,
                    ),
                    F.expr("INTERVAL 1 HOUR"),
                )
            ),
        )
        # Set the quantity to 0 for all the new hour rows created by the explode
        .withColumn(
            ContractColumnNames.quantity,
            F.when(
                F.col(ContractColumnNames.observation_time) == F.col(ContractColumnNames.date),
                F.col(ContractColumnNames.quantity),
            ).otherwise(F.lit(0)),
        )
    )

    return df


def _add_storage_columns(
    measurements: DataFrame,
    orchestration_instance_id: UUID,
    orchestration_type: OrchestrationType,
    metering_point_type: MeteringPointType,
    time_zone: str,
    transaction_creation_datetime: datetime,
) -> CalculatedMeasurementsInternal:
    df = measurements.withColumns(
        {
            ContractColumnNames.orchestration_instance_id: F.lit(str(orchestration_instance_id)),
            ContractColumnNames.orchestration_type: F.lit(orchestration_type.value),
            ContractColumnNames.metering_point_type: F.lit(metering_point_type.value),
            ContractColumnNames.transaction_creation_datetime: F.lit(transaction_creation_datetime),
        }
    )

    return CalculatedMeasurementsInternal(df)


def _resolution_to_interval(resolution: Column) -> Column:
    return (
        F.when(resolution == F.lit(MeteringPointResolution.HOUR.value), F.expr("INTERVAL 1 HOUR"))
        .when(resolution == F.lit(MeteringPointResolution.MONTH.value), F.expr("INTERVAL 1 MONTH"))
        .when(resolution == F.lit(MeteringPointResolution.QUARTER.value), F.expr("INTERVAL 1 QUARTER"))
    )


def _create_transaction_time_column(df) -> DataFrame:
    """Create transaction start and end time columns based on the observation time and resolution.

    This must be done after the transaction id column is created, as it uses the transaction id to group the data.
    The start time is the minimum observation time for each transaction, and the end time is the maximum observation time plus the resolution interval.

    Args:
        df (DataFrame): The DataFrame containing the measurements with a transaction_id column.

    Returns:
        DataFrame: The DataFrame with the transaction start and end time columns added.
    """
    w = Window.partitionBy(ContractColumnNames.transaction_id)
    df_with_start = df.withColumn(
        ContractColumnNames.transaction_start_time,
        F.min(F.col(ContractColumnNames.observation_time)).over(w),
    )
    df_with_end = df_with_start.withColumn(
        ContractColumnNames.transaction_end_time,
        F.max(F.col(ContractColumnNames.observation_time)).over(w)
        + _resolution_to_interval(F.col(ContractColumnNames.resolution)),
    )
    return df_with_end


def _create_transaction_id_column(orchestration_instance_id: UUID, time_zone: str) -> Column:
    """Create a unique transaction id based on the orchestration instance id, metering point id. If there are gaps in the dates a new transaction id is created.

    The id is a UUID5 based on the transaction id string, which makes it deterministic.

    """
    window_spec = Window.partitionBy(ContractColumnNames.metering_point_id).orderBy(F.col(ContractColumnNames.date))

    # Convert 'date' to local time, so that dates subtract '1 DAY' correctly also for daylight saving time
    local_date = F.from_utc_timestamp(F.col(ContractColumnNames.date), time_zone)

    # Identify gaps in the data using local time
    gap = F.when(F.lag(local_date).over(window_spec) != local_date - F.expr("INTERVAL 1 DAY"), 1).otherwise(0)

    transaction_group = F.sum(gap).over(window_spec.rowsBetween(Window.unboundedPreceding, 0))

    transaction_id_str = F.concat_ws(
        "_",
        F.lit(str(orchestration_instance_id)),
        F.col(ContractColumnNames.metering_point_id),
        transaction_group,
    )

    transaction_id_uuid = F.udf(lambda x: str(uuid.uuid5(UUID_NAMESPACE, x)), T.StringType())(transaction_id_str)

    return transaction_id_uuid.alias(ContractColumnNames.transaction_id)
