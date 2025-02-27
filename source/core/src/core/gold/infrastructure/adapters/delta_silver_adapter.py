from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from core.gold.application.ports.silver_port import SilverPort
from core.settings.silver_settings import SilverSettings


class DeltaSilverAdapter(SilverPort):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream(self, table_name: str, read_options: Optional[dict] = None) -> DataFrame:
        silver_settings = SilverSettings()

        return (
            self.spark.readStream.format("delta")
            .options(**read_options or {})
            .table(f"{silver_settings.silver_database_name}.{table_name}")
        )
