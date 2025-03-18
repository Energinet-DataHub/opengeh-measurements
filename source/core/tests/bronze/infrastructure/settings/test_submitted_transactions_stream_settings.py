import os

from core.bronze.infrastructure.settings import SubmittedTransactionsStreamSettings


def test__submitted_transactions_stream_settings__continuous_streaming_should_be_evaluated():
    # Arrange
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"

    # Act
    actual = SubmittedTransactionsStreamSettings()

    # Assert
    assert actual.continuous_streaming_enabled is True
