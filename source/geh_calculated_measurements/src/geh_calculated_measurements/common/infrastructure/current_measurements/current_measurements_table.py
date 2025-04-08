import pyspark.sql.types as t

from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.common.infrastructure.current_measurements.table import Table


class CurrentMeasurementsTable(Table):
    def __init__(self, catalog_name: str) -> None:
        self.fully_qualified_name = f"{catalog_name}.{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}.{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}"
        # super().__init__()

    metering_point_id = t.StructField("metering_point_id", t.StringType(), False)
    observation_time = t.StructField("observation_time", t.TimestampType(), False)
    quantity = t.StructField("quantity", t.DecimalType(18, 3), False)
    quality = t.StructField("quality", t.StringType(), False)
    metering_point_type = t.StructField("metering_point_type", t.StringType(), False)
