import os

from core.settings.gold_settings import GoldSettings


def test__gold_settings__returns_expected():
    # Arrange
    expected_gold_database_name = os.getenv("GOLD_DATABASE_NAME")
    expected_gold_container_name = os.getenv("GOLD_CONTAINER_NAME")

    # Act
    actual = GoldSettings()

    # Assert
    assert actual.gold_database_name == expected_gold_database_name
    assert actual.gold_container_name == expected_gold_container_name
