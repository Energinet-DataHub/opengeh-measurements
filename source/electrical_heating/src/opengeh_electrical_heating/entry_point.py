import os

from opengeh_electrical_heating.application import (
    execute,
)


def execute() -> None:
    applicationinsights_connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")

    execute(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )
