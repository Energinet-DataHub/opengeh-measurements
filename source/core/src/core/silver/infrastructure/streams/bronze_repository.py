from pyspark.sql import DataFrame, SparkSession

from core.settings.catalog_settings import CatalogSettings
from core.silver.infrastructure.config.bronze_calculated_options import BRONZE_CALCULATED_OPTIONS


class BronzeRepository:
    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark

    def read_calculated_measurements(self) -> DataFrame:
        options = BRONZE_CALCULATED_OPTIONS
        catalog_settings = CatalogSettings()  # type: ignore

        raise NotImplementedError("BronzeRepository.read_calculated_measurements")
        source_table_name = f"{catalog_settings.bronze_database_name}"
        return self._spark.readStream.format("delta").options(**options).table(source_table_name)
