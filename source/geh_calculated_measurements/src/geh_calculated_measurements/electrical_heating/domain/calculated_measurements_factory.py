from uuid import UUID

from geh_common.domain.types import MeteringPointType
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.domain.calculated_measurements import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.electrical_heating.domain.column_names import ColumnNames


def create(
    measurements: DataFrame,
    orchestration_instance_id: UUID,
    orchestration_type: str,
    metering_point_type: MeteringPointType,
) -> CalculatedMeasurements:
    df = measurements.withColumns(
        {
            ColumnNames.orchestration_instance_id: F.lit(str(orchestration_instance_id)),
            ColumnNames.orchestration_type: F.lit(orchestration_type),
            ColumnNames.metering_point_type: F.lit(metering_point_type.value),
            ColumnNames.transaction_creation_datetime: F.current_timestamp(),
            ColumnNames.transaction_id: _add_transaction_id(),
        }
    )

    return CalculatedMeasurements(df)


def _add_transaction_id() -> Column:
    # TODO Implement:
    return F.lit("TODO").alias(ColumnNames.transaction_id)
