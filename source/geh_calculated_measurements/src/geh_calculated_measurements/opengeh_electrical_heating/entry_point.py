import os

from geh_calculated_measurements.opengeh_electrical_heating.application import (
    execute_application,
)
import opengeh_electrical_heating.migrations.migrations_runner as migrations_runner


def execute() -> None:
    applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")

    execute_application(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )


def migrate() -> None:
    migrations_runner.migrate()
