from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.missing_measurements_log.domain import (
    MissingMeasurementsLog,
)


class MissingMeasurementsLogRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def write_missing_measurements_log(self, missing_measurements_log: MissingMeasurementsLog) -> None:
        """Write the missing measurements log to the delta table."""
        table_name = f"{self._catalog_name}.{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MISSING_MEASUREMENTS_LOG_TABLE_NAME}"
        missing_measurements_log.df.write.format("delta").mode("append").saveAsTable(table_name)
