from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.domain.constants.columns.bronze_calculated_measurements_column_names import (
    BronzeCalculatedMeasurementsColumnNames,
)
from silver.domain.constants.columns.silver_measurements_column_names import (
    SilverMeasurementsColumnNames,
)


def transform_calculated_measurements(df: DataFrame) -> DataFrame:
    return map_bronze_to_silver(df)


def map_bronze_to_silver(df: DataFrame) -> DataFrame:
    select_list = [
        F.col(BronzeCalculatedMeasurementsColumnNames.orchestration_type).alias(SilverMeasurementsColumnNames.orchestration_type),
        F.col(BronzeCalculatedMeasurementsColumnNames.orchestration_instance_id).alias(
            SilverMeasurementsColumnNames.orchestration_instance_id
        ),
        F.col(BronzeCalculatedMeasurementsColumnNames.metering_point_id).alias(SilverMeasurementsColumnNames.metering_point_id),
        F.col(BronzeCalculatedMeasurementsColumnNames.transaction_id).alias(SilverMeasurementsColumnNames.transaction_id),
        F.col(BronzeCalculatedMeasurementsColumnNames.transaction_creation_datetime).alias(
            SilverMeasurementsColumnNames.transaction_creation_datetime
        ),
        F.col(BronzeCalculatedMeasurementsColumnNames.metering_point_type).alias(
            SilverMeasurementsColumnNames.metering_point_type
        ),
        F.col(BronzeCalculatedMeasurementsColumnNames.product).alias(SilverMeasurementsColumnNames.product),
        F.col(BronzeCalculatedMeasurementsColumnNames.unit).alias(SilverMeasurementsColumnNames.unit),
        F.col(BronzeCalculatedMeasurementsColumnNames.resolution).alias(SilverMeasurementsColumnNames.resolution),
        F.col(BronzeCalculatedMeasurementsColumnNames.start_datetime).alias(SilverMeasurementsColumnNames.start_datetime),
        F.col(BronzeCalculatedMeasurementsColumnNames.end_datetime).alias(SilverMeasurementsColumnNames.end_datetime),
        F.col(BronzeCalculatedMeasurementsColumnNames.points).alias(SilverMeasurementsColumnNames.points),
        F.to_utc_timestamp(F.current_timestamp(), "UTC").alias(SilverMeasurementsColumnNames.created),
    ]

    return df.select(select_list)
