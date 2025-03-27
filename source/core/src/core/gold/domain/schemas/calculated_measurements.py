from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.gold.domain.constants.column_names.calculated_measurements_column_names import (
    CalculatedMeasurementsColumnNames,
)

calculated_measurements_schema = StructType(
    [
        StructField(CalculatedMeasurementsColumnNames.metering_point_id, StringType(), True),
        StructField(CalculatedMeasurementsColumnNames.metering_point_type, StringType(), True),
        StructField(CalculatedMeasurementsColumnNames.observation_time, TimestampType(), True),
        StructField(CalculatedMeasurementsColumnNames.orchestration_instance_id, StringType(), True),
        StructField(CalculatedMeasurementsColumnNames.orchestration_type, StringType(), True),
        StructField(CalculatedMeasurementsColumnNames.quantity, DecimalType(18, 3), True),
        StructField(CalculatedMeasurementsColumnNames.quantity_quality, StringType(), True),
        StructField(CalculatedMeasurementsColumnNames.quantity_unit, StringType(), True),
        StructField(CalculatedMeasurementsColumnNames.resolution, StringType(), True),
        StructField(
            CalculatedMeasurementsColumnNames.transaction_creation_datetime,
            TimestampType(),
            True,
        ),
        StructField(CalculatedMeasurementsColumnNames.transaction_id, StringType(), True),
    ]
)
