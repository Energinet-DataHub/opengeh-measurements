import core.bronze.application.streams.notify_transactions_persisted_stream as notify_transactions_persisted_stream
import core.bronze.application.streams.submitted_transactions as submitted_transactions
import core.bronze.application.batch_scripts.migrate_from_migrations as migrate_from_migrations


def notify_transactions_persisted() -> None:
    notify_transactions_persisted_stream.notify()


def ingest_submitted_transactions() -> None:
    submitted_transactions.submit_transactions()


def migrate_from_migrations_to_measurements() -> None:
    migrate_from_migrations.migrate_from_migrations_to_measurements()
