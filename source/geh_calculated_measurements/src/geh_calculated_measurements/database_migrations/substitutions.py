from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsDatabaseDefinition,
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


def substitutions() -> dict[str, str]:
    return {
        "{calculated_measurements_internal_database}": CalculatedMeasurementsInternalDatabaseDefinition().DATABASE_MEASUREMENTS_CALCULATED_INTERNAL,
        "{calculated_measurements_database}": CalculatedMeasurementsDatabaseDefinition().measurements_calculated_database,
        "{catalog_name}": CatalogSettings().catalog_name,
    }
