import core.silver.application.streams.calculated_stream as calculated_stream
import core.silver.application.streams.notify_transactions_persisted_stream as notify_transactions_persisted_stream
import core.silver.application.streams.submitted_transactions as submitted_transactions


def stream_calculated_measurements() -> None:
    calculated_stream.execute()


def stream_submitted_transactions() -> None:
    submitted_transactions.stream_submitted_transactions()


def notify_transactions_persisted() -> None:
    notify_transactions_persisted_stream.notify()
