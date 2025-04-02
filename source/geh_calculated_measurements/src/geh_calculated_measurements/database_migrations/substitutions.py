from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsDatabaseDefinition,
    CalculatedMeasurementsInternalDatabaseDefinition,
)


def substitutions(catalog_name: str) -> dict[str, str]:
    return {
        "{calculated_measurements_internal_database}": CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_database}": CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
        "{catalog_name}": catalog_name,
    }
