from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from core.gold.domain.constants.column_names.gold_measurements_series_sap_column_names import (
    GoldMeasurementsSeriesSAPColumnNames,
)

gold_measurements_series_sap_schema = StructType(
    [
        StructField(GoldMeasurementsSeriesSAPColumnNames.orchestration_type, StringType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.metering_point_id, StringType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.transaction_id, StringType(), True),
        StructField(
            GoldMeasurementsSeriesSAPColumnNames.transaction_creation_datetime,
            TimestampType(),
            True,
        ),
        StructField(GoldMeasurementsSeriesSAPColumnNames.start_time, TimestampType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.end_time, TimestampType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.unit, StringType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.resolution, StringType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.is_cancelled, BooleanType(), True),
        StructField(GoldMeasurementsSeriesSAPColumnNames.created, TimestampType(), True),
    ]
)
