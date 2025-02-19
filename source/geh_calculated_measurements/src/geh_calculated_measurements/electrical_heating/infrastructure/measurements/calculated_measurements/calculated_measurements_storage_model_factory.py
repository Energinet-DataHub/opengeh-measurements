from uuid import UUID

from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.domain import ColumnNames
from geh_calculated_measurements.electrical_heating.domain.calculated_measurements import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.electrical_heating.infrastructure import CalculatedMeasurementsStorageModel


def create(
    calculated_measurements: CalculatedMeasurements, orchestration_instance_id: UUID, orchestration_type: str
) -> CalculatedMeasurementsStorageModel:
    df = calculated_measurements.df.withColumn(
        ColumnNames.orchestration_instance_id, F.lit(str(orchestration_instance_id))
    ).withColumn(ColumnNames.orchestration_type, F.lit(orchestration_type))

    return CalculatedMeasurementsStorageModel(df)
