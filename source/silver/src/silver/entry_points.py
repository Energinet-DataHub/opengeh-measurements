import silver.application.streams.calculated_stream as calculated_stream
import silver.migrations.migrations as migrations
from silver.infrastructure.helpers.environment_variable_helper import get_applicationinsights_connection_string


def execute_calculated_measurements_silver_stream() -> None:
    calculated_stream.calculated_stream(get_applicationinsights_connection_string())


def migrate() -> None:
    migrations.migrate()
