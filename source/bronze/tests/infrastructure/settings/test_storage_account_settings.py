import os

from opengeh_bronze.infrastructure.settings import (
    StorageAccountSettings,
)


def test__storage_account_settings__should_create_attributes_from_env():
    # Arragnge
    expected_datalake_storage_account = "some_storage_account"
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = expected_datalake_storage_account

    # Act
    actual = StorageAccountSettings()

    # Assert
    assert actual.datalake_storage_account == expected_datalake_storage_account
