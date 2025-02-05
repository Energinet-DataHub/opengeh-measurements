from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.core.gold.application.ports.silver_port import SilverPort
from src.core.gold.infrastructure.shared_helpers import get_full_table_name
from src.core.silver.infrastructure.config import SilverDatabaseNames


class DeltaSilverAdapter(SilverPort):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream(self, table_name: str, read_options: Optional[dict] = None) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .options(**read_options or {})
            .table(get_full_table_name(SilverDatabaseNames.silver, table_name))
        )
