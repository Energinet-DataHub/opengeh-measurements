from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.domain.constants.columns.bronze_measurements_column_names import (
    BronzeMeasurementsColumnNames,
)
from silver.domain.constants.columns.silver_measurements_column_names import (
    SilverMeasurementsColumnNames,
)


def transform(df: DataFrame) -> DataFrame:
    return map_bronze_to_silver(df)


def map_bronze_to_silver(df: DataFrame) -> DataFrame:
    select_list = [
        F.col(BronzeMeasurementsColumnNames.orchestration_type).alias(SilverMeasurementsColumnNames.orchestration_type),
        F.col(BronzeMeasurementsColumnNames.orchestration_instance_id).alias(
            SilverMeasurementsColumnNames.orchestration_instance_id
        ),
        F.col(BronzeMeasurementsColumnNames.metering_point_id).alias(SilverMeasurementsColumnNames.metering_point_id),
        F.col(BronzeMeasurementsColumnNames.transaction_id).alias(SilverMeasurementsColumnNames.transaction_id),
        F.col(BronzeMeasurementsColumnNames.transaction_creation_datetime).alias(
            SilverMeasurementsColumnNames.transaction_creation_datetime
        ),
        F.col(BronzeMeasurementsColumnNames.metering_point_type).alias(
            SilverMeasurementsColumnNames.metering_point_type
        ),
        F.col(BronzeMeasurementsColumnNames.product).alias(SilverMeasurementsColumnNames.product),
        F.col(BronzeMeasurementsColumnNames.unit).alias(SilverMeasurementsColumnNames.unit),
        F.col(BronzeMeasurementsColumnNames.resolution).alias(SilverMeasurementsColumnNames.resolution),
        F.col(BronzeMeasurementsColumnNames.start_datetime).alias(SilverMeasurementsColumnNames.start_datetime),
        F.col(BronzeMeasurementsColumnNames.end_datetime).alias(SilverMeasurementsColumnNames.end_datetime),
        F.col(BronzeMeasurementsColumnNames.points).alias(SilverMeasurementsColumnNames.points),
        F.to_utc_timestamp(F.current_timestamp(), "UTC").alias(SilverMeasurementsColumnNames.created),
    ]

    return df.select(select_list)
