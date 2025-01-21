import bronze.application.migrations as migrations
import bronze.application.submitted_transactions as submitted_transactions


def migrate() -> None:
    migrations.migrate()


def ingest_submitted_transactions() -> None:
    submitted_transactions.submit_transactions()
