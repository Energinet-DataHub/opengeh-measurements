from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import configure_logging

import geh_calculated_measurements.database_migrations.migrations_runner as migrations_runner


def migrate() -> None:
    """Entry point for the database migrations."""
    log_settings = configure_logging(subsystem="measurements", cloud_role_name="dbr-calculated-measurements")

    log = Logger(__name__)
    log.info(f"Initializing migrations with:\nLogging Settings: {log_settings}")

    migrations_runner.migrate()
