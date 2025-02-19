import core.bronze.domain.transformations.submitted_transactions_transformation as submitted_transactions_transformation
import core.bronze.domain.transformations.transactions_persisted_events_transformation as transactions_persisted_events_transformation
from core.bronze.infrastructure.streams.bronze_repository import BronzeRepository
from core.bronze.infrastructure.streams.kafka_stream import KafkaStream


def notify() -> None:
    submitted_transactions = BronzeRepository().read_submitted_transactions()
    unpacked_submitted_transactions = submitted_transactions_transformation.create_by_packed_submitted_transactions(
        submitted_transactions
    )
    events = transactions_persisted_events_transformation.create_by_unpacked_submitted_transactions(
        unpacked_submitted_transactions
    )
    KafkaStream().write_stream(events)
