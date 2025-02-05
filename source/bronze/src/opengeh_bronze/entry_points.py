import opengeh_bronze.migrations.migrations_runner as migrations_runner
import opengeh_bronze.application.batch_scripts.migrate_from_migrations as migrate_from_migrations


def migrate() -> None:
    migrations_runner.migrate()


def migrate_from_migrations_to_measurements() -> None:
    migrate_from_migrations.migrate_from_migrations_to_measurements()
