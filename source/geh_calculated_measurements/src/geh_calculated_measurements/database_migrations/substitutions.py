from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


def substitutions() -> dict[str, str]:
    return {
        "{calculated_measurements_internal_database}": CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_internal_table}": CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME,
        "{catalog_name}": CatalogSettings().catalog_name,
    }
