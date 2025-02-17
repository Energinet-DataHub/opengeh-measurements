import os

from geh_calculated_measurements.electrical_heating.application import (
    execute_application,
)


def execute() -> None:
    applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")

    execute_application(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )
