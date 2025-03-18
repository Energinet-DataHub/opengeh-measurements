from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames

gold_measurements_schema = StructType(
    [
        StructField(GoldMeasurementsColumnNames.metering_point_id, StringType(), True),
        StructField(GoldMeasurementsColumnNames.orchestration_type, StringType(), True),
        StructField(GoldMeasurementsColumnNames.orchestration_instance_id, StringType(), True),
        StructField(GoldMeasurementsColumnNames.observation_time, TimestampType(), True),
        StructField(GoldMeasurementsColumnNames.quantity, DecimalType(18, 3), True),
        StructField(GoldMeasurementsColumnNames.quality, StringType(), True),
        StructField(GoldMeasurementsColumnNames.metering_point_type, StringType(), True),
        StructField(GoldMeasurementsColumnNames.unit, StringType(), True),
        StructField(GoldMeasurementsColumnNames.resolution, StringType(), True),
        StructField(GoldMeasurementsColumnNames.transaction_id, StringType(), True),
        StructField(
            GoldMeasurementsColumnNames.transaction_creation_datetime,
            TimestampType(),
            True,
        ),
        StructField(GoldMeasurementsColumnNames.created, TimestampType(), True),
        StructField(GoldMeasurementsColumnNames.modified, TimestampType(), True),
    ]
)
