import os

from core.settings.silver_settings import SilverSettings


def test__silver_settings__returns_expected():
    # Arrange
    expected_silver_database_name = os.getenv("SILVER_DATABASE_NAME")
    expected_silver_container_name = os.getenv("SILVER_CONTAINER_NAME")

    # Act
    actual = SilverSettings()

    # Assert
    assert actual.silver_database_name == expected_silver_database_name
    assert actual.silver_container_name == expected_silver_container_name
