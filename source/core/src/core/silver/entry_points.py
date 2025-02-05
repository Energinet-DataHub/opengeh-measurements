import src.core.silver.application.streams.calculated_stream as calculated_stream
from src.core.silver.infrastructure.helpers.environment_variable_helper import get_applicationinsights_connection_string


def stream_calculated_measurements() -> None:
    calculated_stream.execute(get_applicationinsights_connection_string())
