import os

from capacity_settlement_2.application.execute_with_deps import execute_with_deps


def execute() -> None:
    applicationinsights_connection_string = os.getenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING"
    )

    execute_with_deps(
        applicationinsights_connection_string=applicationinsights_connection_string,
    )
