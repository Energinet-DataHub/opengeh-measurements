from pyspark.sql import SparkSession

from opengeh_bronze.application.settings import KafkaAuthenticationSettings, SubmittedTransactionsStreamSettings
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames


def submit_transactions(spark: SparkSession) -> None:
    stream_settings = SubmittedTransactionsStreamSettings()  # type: ignore
    kafka_settings = KafkaAuthenticationSettings()  # type: ignore
    kafka_options = kafka_settings.create_kafka_options()

    df = spark.readStream.format("kafka").options(**kafka_options).load()

    dfs = None

    if stream_settings.continuous_streaming_enabled is True:
        dfs = df.writeStream.trigger(continuous="1 second")
    else:
        dfs = df.writeStream.trigger(availableNow=True)

    dfs.toTable(
        f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}", outputMode="append"
    )
