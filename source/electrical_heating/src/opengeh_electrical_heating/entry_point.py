import os

from opengeh_electrical_heating.application.execute_with_deps import (
    execute_with_deps,
)


def execute() -> None:
    applicationinsights_connection_string = os.getenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )

    execute_with_deps(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )
