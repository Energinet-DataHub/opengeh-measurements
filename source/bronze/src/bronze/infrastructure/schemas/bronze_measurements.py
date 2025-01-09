from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from bronze.domain.constants.column_names.bronze_measurements_column_names import (
    BronzeMeasurementsColumnNames,
)

calculation_results_bronze_schema = StructType(
    [
        StructField(BronzeMeasurementsColumnNames.orchestration_type, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.orchestration_instance_id, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.metering_point_id, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.transaction_id, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.transaction_creation_datetime, TimestampType(), True),
        StructField(BronzeMeasurementsColumnNames.metering_point_type, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.product, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.unit, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.resolution, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.start_datetime, TimestampType(), True),
        StructField(BronzeMeasurementsColumnNames.end_datetime, TimestampType(), True),

        StructField(
            BronzeMeasurementsColumnNames.points,
            ArrayType(
                StructType(
                    [
                        StructField(BronzeMeasurementsColumnNames.Points.position, IntegerType(), True),
                        StructField(BronzeMeasurementsColumnNames.Points.quantity, DecimalType(18, 3), True),
                        StructField(BronzeMeasurementsColumnNames.Points.quality, StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),

        StructField(BronzeMeasurementsColumnNames.rescued_data, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.created, TimestampType(), True),
        StructField(BronzeMeasurementsColumnNames.file_path, StringType(), True),
    ]
)
