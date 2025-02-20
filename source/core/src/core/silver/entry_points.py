import core.silver.application.streams.calculated_stream as calculated_stream
import core.silver.application.streams.submitted_transactions as submitted_transactions
import core.silver.application.streams.notify_transactions_persisted_stream as notify_transactions_persisted_stream
from core.utility.environment_variable_helper import get_applicationinsights_connection_string


def stream_calculated_measurements() -> None:
    calculated_stream.execute(get_applicationinsights_connection_string())


def stream_submitted_transactions() -> None:
    submitted_transactions.stream_submitted_transactions()


def notify_transactions_persisted() -> None:
    notify_transactions_persisted_stream.notify()
