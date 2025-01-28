import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.infrastructure.streams.kafka_stream as kafka_stream
from opengeh_bronze.application.settings.submitted_transactions_stream_settings import (
    SubmittedTransactionsStreamSettings,
)


def submit_transactions() -> None:
    spark = spark_session.initialize_spark()
    kafka_options = SubmittedTransactionsStreamSettings().create_kafka_options()
    kafka_stream.submit_transactions(spark, kafka_options)
