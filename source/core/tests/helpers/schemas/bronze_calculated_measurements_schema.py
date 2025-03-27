from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.silver.domain.constants.column_names.bronze_calculated_measurements_column_names import (
    BronzeCalculatedMeasurementsColNames,
)

bronze_calculated_measurements_schema = StructType(
    [
        StructField(BronzeCalculatedMeasurementsColNames.orchestration_type, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.orchestration_instance_id, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.metering_point_id, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.transaction_id, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.transaction_creation_datetime, TimestampType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.metering_point_type, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.product, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.unit, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.resolution, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.start_datetime, TimestampType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.end_datetime, TimestampType(), True),
        StructField(
            BronzeCalculatedMeasurementsColNames.points,
            ArrayType(
                StructType(
                    [
                        StructField(BronzeCalculatedMeasurementsColNames.Points.position, IntegerType(), True),
                        StructField(BronzeCalculatedMeasurementsColNames.Points.quantity, DecimalType(18, 3), True),
                        StructField(BronzeCalculatedMeasurementsColNames.Points.quality, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(BronzeCalculatedMeasurementsColNames.rescued_data, StringType(), True),
        StructField(BronzeCalculatedMeasurementsColNames.created, TimestampType(), True),
    ]
)
