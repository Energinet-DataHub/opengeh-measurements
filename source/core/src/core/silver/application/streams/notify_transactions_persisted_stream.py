import core.silver.domain.transformations.transactions_persisted_events_transformation as transactions_persisted_events_transformation
import core.silver.infrastructure.config.spark_session as spark_session
from core.silver.infrastructure.streams.process_manager_stream import ProcessManagerStream
from core.silver.infrastructure.streams.submitted_transactions_repository import SubmittedTransactionsRepository


def notify() -> None:
    spark = spark_session.initialize_spark()
    submitted_transactions = SubmittedTransactionsRepository(spark).read_submitted_transactions()
    events = transactions_persisted_events_transformation.transform(submitted_transactions)
    ProcessManagerStream().write_stream(events)
