import os

from core.settings.bronze_settings import BronzeSettings


def test__bronze_settings__returns_expected():
    # Arrange
    expected_bronze_database_name = os.getenv("BRONZE_DATABASE_NAME")
    expected_bronze_container_name = os.getenv("BRONZE_CONTAINER_NAME")

    # Act
    actual = BronzeSettings()

    # Assert
    assert actual.bronze_database_name == expected_bronze_database_name
    assert actual.bronze_container_name == expected_bronze_container_name
