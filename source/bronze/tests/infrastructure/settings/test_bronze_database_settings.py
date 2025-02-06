import os

from opengeh_bronze.infrastructure.settings import (
    CatalogSettings,
)


def test__catalog_settings__should_create_attributes_from_env():
    # Arrange
    expected_bronze_database_name = "some_database_name"
    os.environ["BRONZE_DATABASE_NAME"] = expected_bronze_database_name

    # Act
    actual = CatalogSettings()

    # Assert
    assert actual.bronze_database_name == expected_bronze_database_name
