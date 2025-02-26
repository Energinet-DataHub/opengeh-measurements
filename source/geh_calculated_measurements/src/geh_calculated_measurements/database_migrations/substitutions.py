from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition


def substitutions() -> dict[str, str]:
    return {
        "{calculated_measurements_database}": CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_table}": CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_NAME,
    }
