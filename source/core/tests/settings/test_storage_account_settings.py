import os

from core.settings import StorageAccountSettings


def test__submitted_transactions_stream_settings__continuous_streaming_should_be_evaluated():
    # Arrange
    expected_datalake_storage_account = "some_storage_account"
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = expected_datalake_storage_account

    # Act
    actual = StorageAccountSettings()

    # Assert
    assert actual.DATALAKE_STORAGE_ACCOUNT == expected_datalake_storage_account
