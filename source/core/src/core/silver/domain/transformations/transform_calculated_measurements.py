from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.silver.domain.constants.column_names.bronze_calculated_measurements_column_names import (
    BronzeCalculatedMeasurementsColNames,
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames


def transform_calculated_measurements(df: DataFrame) -> DataFrame:
    select_list = [
        F.col(BronzeCalculatedMeasurementsColNames.orchestration_type).alias(
            SilverMeasurementsColumnNames.orchestration_type
        ),
        F.col(BronzeCalculatedMeasurementsColNames.orchestration_instance_id).alias(
            SilverMeasurementsColumnNames.orchestration_instance_id
        ),
        F.col(BronzeCalculatedMeasurementsColNames.metering_point_id).alias(
            SilverMeasurementsColumnNames.metering_point_id
        ),
        F.col(BronzeCalculatedMeasurementsColNames.transaction_id).alias(SilverMeasurementsColumnNames.transaction_id),
        F.col(BronzeCalculatedMeasurementsColNames.transaction_creation_datetime).alias(
            SilverMeasurementsColumnNames.transaction_creation_datetime
        ),
        F.col(BronzeCalculatedMeasurementsColNames.metering_point_type).alias(
            SilverMeasurementsColumnNames.metering_point_type
        ),
        F.col(BronzeCalculatedMeasurementsColNames.unit).alias(SilverMeasurementsColumnNames.unit),
        F.col(BronzeCalculatedMeasurementsColNames.resolution).alias(SilverMeasurementsColumnNames.resolution),
        F.col(BronzeCalculatedMeasurementsColNames.start_datetime).alias(SilverMeasurementsColumnNames.start_datetime),
        F.col(BronzeCalculatedMeasurementsColNames.end_datetime).alias(SilverMeasurementsColumnNames.end_datetime),
        F.col(BronzeCalculatedMeasurementsColNames.points).alias(SilverMeasurementsColumnNames.points),
        F.lit(False).alias(SilverMeasurementsColumnNames.is_cancelled),
        F.to_utc_timestamp(F.current_timestamp(), "UTC").alias(SilverMeasurementsColumnNames.created),
    ]

    return df.select(select_list)
