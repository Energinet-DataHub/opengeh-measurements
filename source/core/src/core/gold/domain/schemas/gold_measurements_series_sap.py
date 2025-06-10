from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from core.gold.domain.constants.column_names.gold_measurements_sap_series_column_names import (
    GoldMeasurementsSAPSeriesColumnNames,
)

gold_measurements_sap_series_schema = StructType(
    [
        StructField(GoldMeasurementsSAPSeriesColumnNames.orchestration_type, StringType(), True),
        StructField(GoldMeasurementsSAPSeriesColumnNames.metering_point_id, StringType(), True),
        StructField(GoldMeasurementsSAPSeriesColumnNames.transaction_id, StringType(), True),
        StructField(
            GoldMeasurementsSAPSeriesColumnNames.transaction_creation_datetime,
            TimestampType(),
            True,
        ),
        StructField(GoldMeasurementsSAPSeriesColumnNames.start_time, TimestampType(), True),
        StructField(GoldMeasurementsSAPSeriesColumnNames.end_time, TimestampType(), True),
        StructField(GoldMeasurementsSAPSeriesColumnNames.unit, StringType(), True),
        StructField(GoldMeasurementsSAPSeriesColumnNames.resolution, StringType(), True),
        StructField(GoldMeasurementsSAPSeriesColumnNames.created, TimestampType(), True),
    ]
)
