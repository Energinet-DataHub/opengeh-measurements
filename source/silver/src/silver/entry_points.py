import silver.application.stream as silver_stream
import silver.migrations.migrations as migrations
from silver.infrastructure.helpers.environment_variable_helper import get_applicationinsights_connection_string


def execute_calculated_measurements_silver_stream() -> None:
    silver_stream.execute_calculated_silver_stream(get_applicationinsights_connection_string())


def migrate() -> None:
    migrations.migrate()
