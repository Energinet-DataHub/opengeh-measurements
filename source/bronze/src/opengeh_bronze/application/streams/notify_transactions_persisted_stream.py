import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.domain.transformations.transactions_persisted_events_transformation as transactions_persisted_events_transformation
from opengeh_bronze.infrastructure.streams.bronze_repository import BronzeRepository
from opengeh_bronze.infrastructure.streams.kafka_stream import KafkaStream


def notify() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = BronzeRepository(spark).read_submitted_transactions()
    events = transactions_persisted_events_transformation.transform(submitted_transactions)
    KafkaStream().write_stream(events)
