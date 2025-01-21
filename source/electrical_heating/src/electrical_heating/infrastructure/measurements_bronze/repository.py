from pyspark.sql import DataFrame, SparkSession

from electrical_heating.infrastructure.measurements_bronze.database_definitions import (
    MeasurementsBronzeDatabase,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name
        self._database_name = MeasurementsBronzeDatabase.DATABASE_NAME
        self._measurements_table_name = MeasurementsBronzeDatabase.MEASUREMENTS_NAME

    def write_measurements(self, df: DataFrame) -> None:
        df.writeTo(
            f"{self._catalog_name}.{self._database_name}.{self._measurements_table_name}"
        )

    def read_measurements(self) -> DataFrame:
        return self._spark.read.table(
            f"{self._catalog_name}.{self._database_name}.{self._measurements_table_name}"
        )
