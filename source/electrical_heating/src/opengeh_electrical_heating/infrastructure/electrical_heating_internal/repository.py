from pyspark.sql import SparkSession, DataFrame

from opengeh_electrical_heating.infrastructure.electricity_market.database_definitions import (
    Database,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
    ) -> None:
        self._spark = spark
        self._catalog_name = catalog_name

    def save(self, calculation: DataFrame) -> None:
        # TODO Will implemented in another PR.
        pass

    def _read_view_or_table(
        self,
        table_name: str,
    ) -> DataFrame:
        name = f"{self._catalog_name}.{Database.DATABASE_NAME}.{table_name}"
        return self._spark.read.format("delta").table(name)
