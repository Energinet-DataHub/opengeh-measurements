from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver.domain.constants.col_names_silver_measurements import SilverMeasurementsColNames

silver_measurements_schema = StructType(
    [
        StructField(SilverMeasurementsColNames.orchestration_type, StringType(), True),
        StructField(SilverMeasurementsColNames.orchestration_instance_id, StringType(), True),
        StructField(SilverMeasurementsColNames.metering_point_id, StringType(), True),
        StructField(SilverMeasurementsColNames.transaction_id, StringType(), True),
        StructField(SilverMeasurementsColNames.transaction_creation_datetime, TimestampType(), True),
        StructField(SilverMeasurementsColNames.metering_point_type, StringType(), True),
        StructField(SilverMeasurementsColNames.product, StringType(), True),
        StructField(SilverMeasurementsColNames.unit, StringType(), True),
        StructField(SilverMeasurementsColNames.resolution, StringType(), True),
        StructField(SilverMeasurementsColNames.start_datetime, TimestampType(), True),
        StructField(SilverMeasurementsColNames.end_datetime, TimestampType(), True),
        StructField(
            SilverMeasurementsColNames.points,
            ArrayType(
                StructType(
                    [
                        StructField(SilverMeasurementsColNames.Points.position, IntegerType(), True),
                        StructField(SilverMeasurementsColNames.Points.quantity, DecimalType(18, 3), True),
                        StructField(SilverMeasurementsColNames.Points.quality, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(SilverMeasurementsColNames.created, TimestampType(), True),
    ]
)
