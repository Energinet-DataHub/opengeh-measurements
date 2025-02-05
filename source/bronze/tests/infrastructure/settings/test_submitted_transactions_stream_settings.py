import os

from opengeh_bronze.infrastructure.settings import (
    SubmittedTransactionsStreamSettings,
)


def test__submitted_transactions_stream_settings__should_create_attributes_from_env():
    # Arragnge
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"

    # Act
    actual = SubmittedTransactionsStreamSettings()

    # Assert
    assert actual.continuous_streaming_enabled is True
