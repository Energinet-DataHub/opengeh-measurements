import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from core.gold.domain.constants.column_names.calculated_measurements_column_names import (
    CalculatedMeasurementsColumnNames,
)
from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames


def transform_calculated_to_gold(df: DataFrame) -> DataFrame:
    return df.select(
        F.col(CalculatedMeasurementsColumnNames.metering_point_id).alias(GoldMeasurementsColumnNames.metering_point_id),
        F.col(CalculatedMeasurementsColumnNames.orchestration_type).alias(
            GoldMeasurementsColumnNames.orchestration_type
        ),
        F.col(CalculatedMeasurementsColumnNames.orchestration_instance_id).alias(
            GoldMeasurementsColumnNames.orchestration_instance_id
        ),
        F.col(CalculatedMeasurementsColumnNames.observation_time).alias(GoldMeasurementsColumnNames.observation_time),
        F.col(CalculatedMeasurementsColumnNames.quantity).alias(GoldMeasurementsColumnNames.quantity),
        F.col(CalculatedMeasurementsColumnNames.quantity_quality).alias(GoldMeasurementsColumnNames.quality),
        F.col(CalculatedMeasurementsColumnNames.metering_point_type).alias(
            GoldMeasurementsColumnNames.metering_point_type
        ),
        F.col(CalculatedMeasurementsColumnNames.quantity_unit).alias(GoldMeasurementsColumnNames.unit),
        F.col(CalculatedMeasurementsColumnNames.resolution).alias(GoldMeasurementsColumnNames.resolution),
        F.col(CalculatedMeasurementsColumnNames.transaction_id).alias(GoldMeasurementsColumnNames.transaction_id),
        F.col(CalculatedMeasurementsColumnNames.transaction_creation_datetime).alias(
            GoldMeasurementsColumnNames.transaction_creation_datetime
        ),
        F.lit(False).alias(GoldMeasurementsColumnNames.is_cancelled),
        F.current_timestamp().alias(GoldMeasurementsColumnNames.created),
        F.current_timestamp().alias(GoldMeasurementsColumnNames.modified),
    )
