from core.receipts.domain.transformations.transactions_persisted_events_transformation import transform
from core.receipts.infrastructure.repositories.receipts_repository import ReceiptsRepository
from core.receipts.infrastructure.streams.process_manager_stream import ProcessManagerStream


def notify() -> None:
    events = ReceiptsRepository().read()
    transformed_events = transform(events)
    ProcessManagerStream().write_stream(transformed_events)
