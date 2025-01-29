from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opengeh_silver.domain.config.column_names import BronzeCalculatedMeasurementsColNames, SilverMeasurementsColNames


def transform_calculated_measurements(df: DataFrame) -> DataFrame:
    return map_bronze_to_silver(df)


def map_bronze_to_silver(df: DataFrame) -> DataFrame:
    select_list = [
        F.col(BronzeCalculatedMeasurementsColNames.orchestration_type).alias(
            SilverMeasurementsColNames.orchestration_type
        ),
        F.col(BronzeCalculatedMeasurementsColNames.orchestration_instance_id).alias(
            SilverMeasurementsColNames.orchestration_instance_id
        ),
        F.col(BronzeCalculatedMeasurementsColNames.metering_point_id).alias(
            SilverMeasurementsColNames.metering_point_id
        ),
        F.col(BronzeCalculatedMeasurementsColNames.transaction_id).alias(SilverMeasurementsColNames.transaction_id),
        F.col(BronzeCalculatedMeasurementsColNames.transaction_creation_datetime).alias(
            SilverMeasurementsColNames.transaction_creation_datetime
        ),
        F.col(BronzeCalculatedMeasurementsColNames.metering_point_type).alias(
            SilverMeasurementsColNames.metering_point_type
        ),
        F.col(BronzeCalculatedMeasurementsColNames.product).alias(SilverMeasurementsColNames.product),
        F.col(BronzeCalculatedMeasurementsColNames.unit).alias(SilverMeasurementsColNames.unit),
        F.col(BronzeCalculatedMeasurementsColNames.resolution).alias(SilverMeasurementsColNames.resolution),
        F.col(BronzeCalculatedMeasurementsColNames.start_datetime).alias(SilverMeasurementsColNames.start_datetime),
        F.col(BronzeCalculatedMeasurementsColNames.end_datetime).alias(SilverMeasurementsColNames.end_datetime),
        F.col(BronzeCalculatedMeasurementsColNames.points).alias(SilverMeasurementsColNames.points),
        F.to_utc_timestamp(F.current_timestamp(), "UTC").alias(SilverMeasurementsColNames.created),
    ]

    return df.select(select_list)
