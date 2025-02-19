import os

<<<<<<< HEAD:source/geh_calculated_measurements/src/geh_calculated_measurements/opengeh_electrical_heating/entry_point.py
import geh_calculated_measurements.opengeh_electrical_heating.migrations.migrations_runner as migrations_runner
from geh_calculated_measurements.opengeh_electrical_heating.application import (
=======
from geh_calculated_measurements.electrical_heating.application import (
>>>>>>> 5a82a1ba50f0370ac84bf75648e1cafdbc81b607:source/geh_calculated_measurements/src/geh_calculated_measurements/electrical_heating/entry_point.py
    execute_application,
)


def execute() -> None:
    # Entry point for the Electrical Heating
    applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")

    execute_application(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )


def migrate() -> None:
    # Entry point for the database migrations
    migrations_runner.migrate()
