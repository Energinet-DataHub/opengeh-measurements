import os

from core.settings.catalog_settings import CatalogSettings


def test__catalog_settings__returns_expected():
    # Arrange
    expected_catalog_name = os.getenv("CATALOG_NAME")

    # Act
    actual = CatalogSettings()

    # Assert
    assert actual.catalog_name == expected_catalog_name
