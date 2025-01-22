from typing import Optional

from pyspark.sql import SparkSession, DataFrame

from gold.application.ports.silver_repository import SilverRepository
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.shared_helpers import get_full_table_name


class DeltaSilverRepository(SilverRepository):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream(self, table_name: str, read_options: Optional[dict] = None) -> DataFrame:
        return (self.spark.readStream
                .format("delta")
                .options(**read_options or {})
                .table(get_full_table_name(DatabaseNames.silver_database, table_name)))
