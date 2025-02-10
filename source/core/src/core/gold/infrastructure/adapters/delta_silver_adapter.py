from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from core.gold.application.ports.silver_port import SilverPort
from core.settings.catalog_settings import CatalogSettings
from core.utility.shared_helpers import get_full_table_name


class DeltaSilverAdapter(SilverPort):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream(self, table_name: str, read_options: Optional[dict] = None) -> DataFrame:
        catalog_settings = CatalogSettings()  # type: ignore

        return (
            self.spark.readStream.format("delta")
            .options(**read_options or {})
            .table(get_full_table_name(catalog_settings.silver_database_name, table_name))
        )
