from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ArrayType, IntegerType, DecimalType
)
from database_migration.constants.bronze_measurements_constants import BronzeMeasurementsConstants


calculation_results_bronze_schema = StructType(
    [
        StructField(BronzeMeasurementsConstants.orchestration_type, StringType(), False),
        StructField(BronzeMeasurementsConstants.orchestration_instance_id, StringType(), False),
        StructField(BronzeMeasurementsConstants.metering_point_id, StringType(), False),
        StructField(BronzeMeasurementsConstants.transaction_id, StringType(), False),
        StructField(BronzeMeasurementsConstants.transaction_creation_datetime, TimestampType(), False),
        StructField(BronzeMeasurementsConstants.metering_point_type, StringType(), False),
        StructField(BronzeMeasurementsConstants.product, StringType(), False),
        StructField(BronzeMeasurementsConstants.unit, StringType(), False),
        StructField(BronzeMeasurementsConstants.resolution, StringType(), False),
        StructField(BronzeMeasurementsConstants.start_datetime, TimestampType(), False),
        StructField(BronzeMeasurementsConstants.end_datetime, TimestampType(), False),

        StructField(
            BronzeMeasurementsConstants.points,
            ArrayType(
                StructType(
                    [
                        StructField(BronzeMeasurementsConstants.Points.position, IntegerType(), True),
                        StructField(BronzeMeasurementsConstants.Points.quantity, DecimalType(18, 6), True),
                        StructField(BronzeMeasurementsConstants.Points.quality, StringType(), True),
                    ]
                ),
                True,
            ),
            False,
        ),

        StructField(BronzeMeasurementsConstants.rescued_data, StringType(), True),
        StructField(BronzeMeasurementsConstants.created, TimestampType(), False),
        StructField(BronzeMeasurementsConstants.file_path, StringType(), False),
    ]
)
