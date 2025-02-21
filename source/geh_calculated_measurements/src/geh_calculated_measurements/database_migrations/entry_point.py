import geh_calculated_measurements.database_migrations.migrations_runner as migrations_runner


def migrate() -> None:
    """Entry point for the database migrations."""
    migrations_runner.migrate()
