from pyspark.sql import SparkSession

from opengeh_bronze.application.settings.submitted_transactions_stream_settings import (
    SubmittedTransactionsStreamSettings,
)
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames


def submit_transactions(spark: SparkSession, kafka_options: dict) -> None:
    stream_settings = SubmittedTransactionsStreamSettings()  # type: ignore

    df = spark.readStream.format("kafka").options(**kafka_options).load()
    dfs = None

    if stream_settings.continuous_streaming_enabled is True:
        dfs = df.writeStream.trigger(continuous="1 second")
    else:
        dfs = df.writeStream.trigger(once=True)

    dfs.toTable(
        f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}", outputMode="append"
    )
