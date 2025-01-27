import bronze.application.streams.publish_receipt_stream as publish_receipt_stream
import bronze.migrations.migrations_runner as migrations_runner


def migrate() -> None:
    migrations_runner.migrate()


def receipt_stream() -> None:
    publish_receipt_stream.stream()
