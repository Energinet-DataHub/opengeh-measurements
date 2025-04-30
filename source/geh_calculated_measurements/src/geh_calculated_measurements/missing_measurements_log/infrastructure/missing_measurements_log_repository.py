from pyspark.sql import SparkSession

from geh_calculated_measurements.database_migrations import DatabaseNames
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
        table_name = (
            f"{self._catalog_name}.{DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL}.{'missing_measurements_log'}"
        )
        missing_measurements_log.df.write.format("delta").mode("append").saveAsTable(table_name)
