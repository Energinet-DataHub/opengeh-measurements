import pyspark.sql.types as t

from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.common.infrastructure.current_measurements.table import Table


class CurruntMeasurements(Table):
    def __init__(self) -> None:
        super().__init__()

    fully_quallified_name = f"spark_catalog.{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
    metering_point_id = t.StructField("metering_point_id", t.StringType(), not Table.nullable)
    observation_time = t.StructField("observation_time", t.TimestampType(), not Table.nullable)
    quantity = t.StructField("quantity", t.DecimalType(18, 3), not Table.nullable)
    quality = (t.StructField("quality", t.StringType(), Table.nullable),)
    metering_point_type = (t.StructField("metering_point_type", t.StringType(), Table.nullable),)
