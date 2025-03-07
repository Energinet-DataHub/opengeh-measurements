from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.common.infrastructure.calculated_measurements.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str | None = None,
    ) -> None:
        self._spark = spark
        self._calculated_measurements_database_name = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        self._calculated_measurements_table_name = CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME
        self._catalog_name = catalog_name
        if self._catalog_name:
            self._calculated_measurements_full_table_path = f"{self._catalog_name}.{self._calculated_measurements_database_name}.{self._calculated_measurements_table_name}"
        else:
            self._calculated_measurements_full_table_path = (
                f"{self._calculated_measurements_database_name}.{self._calculated_measurements_table_name}"
            )

    def write_calculated_measurements(
        self, calculated_measurements: CalculatedMeasurements, write_mode: str = "append"
    ) -> None:
        calculated_measurements.df.write.format("delta").mode(write_mode).saveAsTable(
            self._calculated_measurements_full_table_path
        )

    def read_calculated_measurements(self) -> CalculatedMeasurements:
        return CalculatedMeasurements(self._spark.read.table(self._calculated_measurements_full_table_path))
