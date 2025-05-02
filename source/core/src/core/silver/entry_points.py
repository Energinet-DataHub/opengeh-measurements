import core.silver.application.streams.submitted_transactions as submitted_transactions


def stream_submitted_transactions() -> None:
    submitted_transactions.stream_submitted_transactions()
