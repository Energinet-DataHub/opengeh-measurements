import core.bronze.application.streams.submitted_transactions as submitted_transactions


def ingest_submitted_transactions() -> None:
    submitted_transactions.submit_transactions()
