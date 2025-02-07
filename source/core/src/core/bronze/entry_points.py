import core.bronze.application.streams.notify_transactions_persisted_stream as notify_transactions_persisted_stream
import core.bronze.application.streams.submitted_transactions as submitted_transactions


def notify_transactions_persisted() -> None:
    notify_transactions_persisted_stream.notify()


def ingest_submitted_transactions() -> None:
    submitted_transactions.submit_transactions()
