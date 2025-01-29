import opengeh_silver.application.streams.calculated_stream as calculated_stream
import opengeh_silver.migrations.migrations as migrations
from opengeh_silver.infrastructure.helpers.environment_variable_helper import get_applicationinsights_connection_string


def stream_calculated_measurements() -> None:
    calculated_stream.execute(get_applicationinsights_connection_string())


def migrate() -> None:
    migrations.migrate()
