from pyspark.sql import DataFrame

import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.domain.transformations.bronze_measurements_transformations as bronze_measurements_transformations
import opengeh_bronze.domain.transformations.notify_transactions_persisted_events_transformation as notify_transactions_persisted_events_transformation
import opengeh_bronze.infrastructure.streams.writer as writer
from opengeh_bronze.infrastructure.streams.bronze_repository import BronzeRepository
from opengeh_bronze.infrastructure.streams.kafka_stream import KafkaStream


def notify() -> None:
    spark = spark_session.initialize_spark()
    bronze_stream = BronzeRepository(spark).read_submitted_transactions()
    options = {"ignoreDeletes": "true"}
    writer.write_stream(bronze_stream, "query_name", options, notify_transactions_persisted)


def notify_transactions_persisted(bronze_measurements: DataFrame, batch_id: int) -> None:
    unpackaged_measurements = bronze_measurements_transformations.transform(bronze_measurements)
    notify_transactions_persisted_events = notify_transactions_persisted_events_transformation.transform(
        unpackaged_measurements
    )
    KafkaStream().write_stream(notify_transactions_persisted_events)
