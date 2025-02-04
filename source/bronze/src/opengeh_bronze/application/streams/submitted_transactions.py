import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.infrastructure.streams.kafka_stream as kafka_stream
from opengeh_bronze.application.settings.kafka_authentication_settings import (
    KafkaAuthenticationSettings,
)


def submit_transactions() -> None:
    spark = spark_session.initialize_spark()
    kafka_settings = KafkaAuthenticationSettings()  # type: ignore

    kafka_stream.submit_transactions(spark, kafka_settings.create_kafka_options())
