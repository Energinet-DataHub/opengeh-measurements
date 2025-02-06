import os

from opengeh_gold.infrastructure.settings.catalog_settings import CatalogSettings


def test__bronze_database_settings__should_create_attributes_from_env():
    # Arrange
    expected_catalog_name = "some_catalog_name"
    expected_bronze_database_name = "some_bronze_database_name"
    expected_silver_database_name = "some_silver_database_name"
    expected_gold_database_name = "some_gold_database_name"
    os.environ["catalog_name"] = expected_catalog_name
    os.environ["bronze_database_name"] = expected_bronze_database_name
    os.environ["silver_database_name"] = expected_silver_database_name
    os.environ["gold_database_name"] = expected_gold_database_name

    # Act
    actual = CatalogSettings()  # type: ignore

    # Assert
    assert actual.catalog_name == expected_catalog_name
    assert actual.bronze_database_name == expected_bronze_database_name
    assert actual.silver_database_name == expected_silver_database_name
    assert actual.gold_database_name == expected_gold_database_name
