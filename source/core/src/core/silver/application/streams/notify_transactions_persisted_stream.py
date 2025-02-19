import core.bronze.application.config.spark_session as spark_session
import core.silver.domain.transformations.transactions_persisted_events_transformation as transactions_persisted_events_transformation
from core.silver.infrastructure.streams.kafka_stream import KafkaStream
from core.silver.infrastructure.streams.submitted_transactions_repository import SubmittedTransactionsRepository


def notify() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = SubmittedTransactionsRepository(spark).read_submitted_transactions()
    events = transactions_persisted_events_transformation.transform(submitted_transactions)
    KafkaStream().write_stream(events)
