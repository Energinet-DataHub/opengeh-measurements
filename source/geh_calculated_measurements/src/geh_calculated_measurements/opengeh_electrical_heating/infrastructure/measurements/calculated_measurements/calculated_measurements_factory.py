from uuid import UUID

from pyspark.sql import DataFrame

from geh_calculated_measurements.opengeh_electrical_heating.infrastructure import CalculatedMeasurements


def create(
    calculated_measurements: DataFrame, orchestration_instance_id: UUID, orchestration_type
) -> CalculatedMeasurements:
    return calculated_measurements.withColumn("orchestration_instance_id", str(orchestration_instance_id)).with_column(
        "orchestration_type", orchestration_type
    )
