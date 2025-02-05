import opengeh_bronze.migrations.migrations_runner as migrations_runner
import opengeh_bronze.infrastructure.batch_scripts.migrate_from_migrations as migrate_from_migrations


def migrate() -> None:
    migrations_runner.migrate()


def migrations_to_measurements() -> None:
    migrate_from_migrations.migrations_to_measurements()
