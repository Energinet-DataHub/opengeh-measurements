import os

from geh_calculated_measurements.opengeh_electrical_heating.settings.catalog_settings import CatalogSettings


def test__catalog_settings__environmental_variables_are_read():
    # Arrange
    expected_catalog_name = os.getenv("CATALOG_NAME")

    # Act
    actual = CatalogSettings()  # type: ignore

    # Assert
    assert actual.catalog_name == expected_catalog_name
