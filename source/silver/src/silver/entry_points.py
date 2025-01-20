import os
import source.silver.src.silver.infrastructure.migrations.migrations as migrations
import source.silver.src.silver.application.stream as silver_stream


def execute_silver_stream() -> None:
    applicationinsights_connection_string = os.getenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )
    silver_stream.execute(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )


def migrate() -> None:
    migrations.migrate()
