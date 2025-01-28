from pyspark.sql import DataFrame, SparkSession

from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.settings.submitted_transactions_stream_settings import (
    SubmittedTransactionsStreamSettings,
)


class KafkaStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = SubmittedTransactionsStreamSettings().create_kafka_options()

    def submit_transactions(self, spark: SparkSession) -> None:
        spark.readStream.format("kafka").options(**self.kafka_options).load().writeTo(
            f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}"
        )

    def write_stream(self, dataframe: DataFrame):
        dataframe.writeStream.format("kafka").option(**self.kafka_options).start().awaitTermination()
