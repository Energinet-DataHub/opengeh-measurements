from pyspark.sql import DataFrame, SparkSession

from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.bronze_settings import BronzeSettings


class SubmittedTransactionsRepository:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_database_name = BronzeSettings().bronze_database_name

    def read(self) -> DataFrame:
        return (
            self.spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .table(f"{self.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}")
        )
