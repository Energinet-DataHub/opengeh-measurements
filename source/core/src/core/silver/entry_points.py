import core.silver.application.streams.calculated_stream as calculated_stream
from core.utility.environment_variable_helper import get_applicationinsights_connection_string


def stream_calculated_measurements() -> None:
    calculated_stream.execute(get_applicationinsights_connection_string())

def stream_submitted_transactions() -> None:
    