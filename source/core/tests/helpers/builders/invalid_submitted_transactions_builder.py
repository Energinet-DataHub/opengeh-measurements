from pyspark.sql import DataFrame, SparkSession

from core.bronze.domain.schemas.invalid_submitted_transactions import invalid_submitted_transactions_schema
from tests.helpers.builders.submitted_transactions_builder import ValueBuilder


class InvalidSubmittedTransactionsBuilder:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data = []

    def add_row(
        self,
        key=None,
        value=None,
        topic=None,
        partition=None,
        offset=None,
        timestamp=None,
        timestampType=None,
        version=None,
    ) -> "InvalidSubmittedTransactionsBuilder":
        if value is None:
            value = ValueBuilder().build()

        self.data.append((key, value, topic, partition, offset, timestamp, timestampType, version))

        return self

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(self.data, schema=invalid_submitted_transactions_schema)
