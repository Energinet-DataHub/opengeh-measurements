import uuid
from uuid import UUID

from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames
from geh_calculated_measurements.common.domain.model.calculated_measurements import (
    CalculatedMeasurements,
)

UUID_NAMESPACE = uuid.UUID("539ba8c3-5d10-4aa9-81d5-632cfce33e18")
""" Define a fixed UUID to use as the namespace for generating UUID v5 values. 
This ensures that all UUIDs generated with this namespace and a given name are stable (i.e., the same input always 
produces the same output)."""


def create(
    measurements: DataFrame,
    orchestration_instance_id: UUID,
    orchestration_type: OrchestrationType,
    metering_point_type: MeteringPointType,
    time_zone: str,
) -> CalculatedMeasurements:
    df = measurements.withColumns(
        {
            ContractColumnNames.orchestration_instance_id: F.lit(str(orchestration_instance_id)),
            ContractColumnNames.orchestration_type: F.lit(orchestration_type.value),
            ContractColumnNames.metering_point_type: F.lit(metering_point_type.value),
            ContractColumnNames.transaction_creation_datetime: F.current_timestamp(),
            ContractColumnNames.transaction_id: _add_transaction_id(orchestration_instance_id, time_zone),
        }
    )

    return CalculatedMeasurements(df)


def _add_transaction_id(orchestration_instance_id: UUID, time_zone: str) -> Column:
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

    transaction_id_uuid = F.udf(lambda x: str(uuid.uuid5(UUID_NAMESPACE, x)), StringType())(transaction_id_str)

    return transaction_id_uuid.alias(ContractColumnNames.transaction_id)
