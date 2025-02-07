import os

from core.settings.catalog_settings import CatalogSettings


def test__submitted_transactions_stream_settings__continuous_streaming_should_be_evaluated():
    # Arrange
    expected_catalog_name = os.getenv("CATALOG_NAME")
    expected_bronze_database_name = os.getenv("BRONZE_DATABASE_NAME")
    expected_silver_database_name = os.getenv("SILVER_DATABASE_NAME")
    expected_gold_database_name = os.getenv("GOLD_DATABASE_NAME")

    # Act
    actual = CatalogSettings()  # type: ignore

    # Assert
    assert actual.catalog_name == expected_catalog_name
    assert actual.bronze_database_name == expected_bronze_database_name
    assert actual.silver_database_name == expected_silver_database_name
    assert actual.gold_database_name == expected_gold_database_name
