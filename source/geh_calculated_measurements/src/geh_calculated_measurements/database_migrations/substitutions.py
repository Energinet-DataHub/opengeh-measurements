from geh_calculated_measurements.electrical_heating.infrastructure import (
    CalculatedMeasurementsDatabaseDefinition,
    MeasurementsGoldDatabaseDefinition,
)


def substitutions() -> dict[str, str]:
    return {
        "{gold_measurements_database}": MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        "{gold_measurements_table}": MeasurementsGoldDatabaseDefinition.TIME_SERIES_POINTS_NAME,
        "{calculated_measurements_database}": CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_table}": CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME,
    }
