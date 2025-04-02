import core.silver.domain.transformations.transactions_persisted_events_transformation as transactions_persisted_events_transformation
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository
from core.silver.infrastructure.streams.process_manager_stream import ProcessManagerStream


def notify() -> None:
    submitted_transactions = SilverMeasurementsRepository().read_submitted()
    events = transactions_persisted_events_transformation.transform(submitted_transactions)
    ProcessManagerStream().write_stream(events)
