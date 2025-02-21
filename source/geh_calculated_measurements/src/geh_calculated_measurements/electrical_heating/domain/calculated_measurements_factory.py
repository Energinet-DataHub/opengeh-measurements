from uuid import UUID

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.domain.calculated_measurements_daily import (
    CalculatedMeasurementsDaily,
)
from geh_calculated_measurements.electrical_heating.domain.column_names import ColumnNames


def create(
    measurements: DataFrame,
    orchestration_instance_id: UUID,
    orchestration_type: str,
    metering_point_type: str,
) -> CalculatedMeasurementsDaily:
    df = measurements.withColumn(ColumnNames.orchestration_type, F.lit(orchestration_type)).withColumns(
        {
            ColumnNames.orchestration_instance_id: F.lit(str(orchestration_instance_id)),
            ColumnNames.orchestration_type: F.lit(orchestration_type),
            ColumnNames.metering_point_type: F.lit(metering_point_type),
            ColumnNames.transaction_creation_datetime: F.current_timestamp(),
            ColumnNames.transaction_id: _add_transaction_id(measurements),
        }
    )

    df = df.withColumn(ColumnNames.transaction_id, _add_transaction_id(df))

    return CalculatedMeasurementsDaily(df)


def _add_transaction_id(df: DataFrame) -> Column:
    # TODO Implement:
    return F.lit("TODO").alias(ColumnNames.transaction_id)
