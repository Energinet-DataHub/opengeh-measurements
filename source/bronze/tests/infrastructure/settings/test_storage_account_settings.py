import os

from opengeh_bronze.infrastructure.settings import (
    StorageAccountSettings,
)


def test__submitted_transactions_stream_settings__continuous_streaming_should_be_evaluated():
    # Arragnge
    expected_datalake_storage_account = "some_storage_account"
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = expected_datalake_storage_account

    # Act
    actual = StorageAccountSettings()

    # Assert
    assert actual.datalake_storage_account == expected_datalake_storage_account
