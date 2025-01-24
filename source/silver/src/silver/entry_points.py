import silver.application.stream as silver_stream
import silver.migrations.migrations as migrations
from silver.infrastructure.services.env_vars_utils import get_applicationinsights_connection_string


def execute_silver_stream() -> None:
    silver_stream.execute(get_applicationinsights_connection_string())


def migrate() -> None:
    migrations.migrate()
