import uuid
from uuid import UUID

from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from geh_calculated_measurements.common.domain.column_names import ColumnNames
from geh_calculated_measurements.common.domain.model.calculated_measurements import (
    CalculatedMeasurements,
)

UUID_NAMESPACE = uuid.UUID("539ba8c3-5d10-4aa9-81d5-632cfce33e18")


def create(
    measurements: DataFrame,
    orchestration_instance_id: UUID,
    orchestration_type: OrchestrationType,
    metering_point_type: MeteringPointType,
) -> CalculatedMeasurements:
    df = measurements.withColumns(
        {
            ColumnNames.orchestration_instance_id: F.lit(str(orchestration_instance_id)),
            ColumnNames.orchestration_type: F.lit(orchestration_type.value),
            ColumnNames.metering_point_type: F.lit(metering_point_type.value),
            ColumnNames.transaction_creation_datetime: F.current_timestamp(),
            ColumnNames.transaction_id: _add_transaction_id(orchestration_instance_id),
        }
    )

    return CalculatedMeasurements(df)


def _add_transaction_id(orchestration_instance_id: UUID) -> Column:
    window_spec = Window.partitionBy(ColumnNames.metering_point_id).orderBy(F.col(ColumnNames.date))

    # Identify gaps in the data
    gap = F.when(
        F.lag(F.col(ColumnNames.date)).over(window_spec) != F.col(ColumnNames.date) - F.expr("INTERVAL 1 DAY"), 1
    ).otherwise(0)

    transaction_group = F.sum(gap).over(window_spec.rowsBetween(Window.unboundedPreceding, 0))

    transaction_id_str = F.concat_ws(
        "_",
        F.lit(str(orchestration_instance_id)),
        F.col(ColumnNames.metering_point_id),
        transaction_group,
    )

    transaction_id_uuid = F.udf(lambda x: str(uuid.uuid5(UUID_NAMESPACE, x)), StringType())(transaction_id_str)

    return transaction_id_uuid.alias(ColumnNames.transaction_id)
