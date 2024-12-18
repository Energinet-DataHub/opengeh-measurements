from pyspark.sql import SparkSession, DataFrame

from electrical_heating.infrastructure.measurements_bronze.database_definitions import (
    MeasurementsBronzeDatabase,
)


class MeasurementsBronzeRepository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def read_measurements(self) -> DataFrame:
        return self._read_view_or_table(
            MeasurementsBronzeDatabase.MEASUREMENTS_NAME,
        )

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{MeasurementsBronzeDatabase.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
