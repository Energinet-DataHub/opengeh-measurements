from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import CalculatedMeasurementsInternal
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition


def _seed_measurements_calculated_internal(
    spark: SparkSession,
    catalog_name: str,
    metering_point_id: str,
    quantity: Decimal,
):
    database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
    table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME

    df = spark.createDataFrame(
        [
            (
                "electrical_heating",
                "00000000-0000-0000-0000-000000000001",
                "test_id",
                datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
                metering_point_id,
                "electrical_heating",
                datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
                quantity,
            )
        ],
        schema=CalculatedMeasurementsInternal.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{database_name}.{table_name}")

    return quantity
