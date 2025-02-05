import os

from opengeh_bronze.infrastructure.settings import (
    BronzeDatabaseSettings,
)


def test__bronze_database_settings__should_create_attributes_from_env():
    # Arrange
    expected_bronze_database_name = "some_database_name"
    os.environ["BRONZE_DATABASE_NAME"] = expected_bronze_database_name

    # Act
    actual = BronzeDatabaseSettings()

    # Assert
    assert actual.bronze_database_name == expected_bronze_database_name
