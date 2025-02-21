import os

from geh_calculated_measurements.electrical_heating.application import (
    execute_application,
)


def execute() -> None:
    """Entry point for the Electrical Heating."""
    applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")

    execute_application(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )


def execute_net_consumption_for_group_6() -> None:
    applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")

    execute_application(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )
