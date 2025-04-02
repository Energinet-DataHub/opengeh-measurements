import os

import pytest

from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings
from tests import SPARK_CATALOG_NAME


def test__catalog_settings__environmental_variables_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(os, "environ", {"CATALOG_NAME": SPARK_CATALOG_NAME})

    # Act
    actual = CatalogSettings()  # type: ignore

    # Assert
    assert actual.catalog_name == SPARK_CATALOG_NAME
