import core.src.silver.application.streams.calculated_stream as calculated_stream
import core.src.silver.migrations.migrations_runner as migrations_runner
from core.src.silver.infrastructure.helpers.environment_variable_helper import get_applicationinsights_connection_string


def stream_calculated_measurements() -> None:
    calculated_stream.execute(get_applicationinsights_connection_string())


def migrate() -> None:
    migrations_runner.migrate()
