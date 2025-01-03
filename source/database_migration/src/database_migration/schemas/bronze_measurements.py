from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ArrayType, IntegerType, DecimalType
)
from database_migration.constants.bronze_measurements_constants import BronzeMeasurementsColumnNames


calculation_results_bronze_schema = StructType(
    [
        StructField(BronzeMeasurementsColumnNames.orchestration_type, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.orchestration_instance_id, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.metering_point_id, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.transaction_id, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.transaction_creation_datetime, TimestampType(), False),
        StructField(BronzeMeasurementsColumnNames.metering_point_type, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.product, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.unit, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.resolution, StringType(), False),
        StructField(BronzeMeasurementsColumnNames.start_datetime, TimestampType(), False),
        StructField(BronzeMeasurementsColumnNames.end_datetime, TimestampType(), False),

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
            False,
        ),

        StructField(BronzeMeasurementsColumnNames.rescued_data, StringType(), True),
        StructField(BronzeMeasurementsColumnNames.created, TimestampType(), False),
        StructField(BronzeMeasurementsColumnNames.file_path, StringType(), False),
    ]
)
