import src.core.migrations.migrations_runner as migrations_runner


def migrate() -> None:
    migrations_runner.migrate()
