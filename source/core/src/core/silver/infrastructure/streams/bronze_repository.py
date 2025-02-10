from pyspark.sql import DataFrame, SparkSession

from core.bronze.infrastructure.config import BronzeTableNames
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

        source_table_name = f"{catalog_settings.catalog_name + '.' if catalog_settings.catalog_name else ''}{catalog_settings.bronze_database_name}.{BronzeTableNames.bronze_measurements_table}"
        return self._spark.readStream.format("delta").options(**options).table(source_table_name)
