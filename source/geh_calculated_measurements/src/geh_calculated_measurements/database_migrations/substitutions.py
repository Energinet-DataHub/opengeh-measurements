from geh_calculated_measurements.database_migrations import MeasurementsCalculatedInternalDatabaseDefinition
from geh_calculated_measurements.electrical_heating.infrastructure import (
    CalculatedMeasurementsDatabaseDefinition,
)


def substitutions() -> dict[str, str]:
    return {
        "{calculated_measurements_database}": CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_table}": CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME,
        "{calculated_measurements_internal_database}": MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database,
        "{calculated_measurements_internal_table}": MeasurementsCalculatedInternalDatabaseDefinition.MEASUREMENTS_NAME,
        "{catalog_name}": "spark_catalog",
    }
