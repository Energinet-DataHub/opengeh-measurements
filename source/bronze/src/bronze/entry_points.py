import bronze.migrations.migrations_runner as migrations_runner
import bronze.application.submitted_transactions as submitted_transactions


def migrate() -> None:
    migrations_runner.migrate()


def ingest_submitted_transactions() -> None:
    submitted_transactions.submit_transactions()
