from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)


def substitutions() -> dict[str, str]:
    return {
        "{calculated_measurements_internal_database}": CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_internal_table}": CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME,
        "{catalog_name}": "spark_catalog",
    }
