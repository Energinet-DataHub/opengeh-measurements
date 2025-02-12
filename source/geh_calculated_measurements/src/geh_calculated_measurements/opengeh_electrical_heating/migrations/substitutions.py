from geh_calculated_measurements.opengeh_electrical_heating.infrastructure import (
    CalculatedMeasurementsDatabase,
    MeasurementsGoldDatabase,
)


def substitutions() -> dict[str, str]:
    return {
        "{gold_measurements_database}": MeasurementsGoldDatabase.DATABASE_NAME,
        "{gold_measurements_table}": MeasurementsGoldDatabase.TIME_SERIES_POINTS_NAME,
        "{calculated_measurements_database}": CalculatedMeasurementsDatabase.DATABASE_NAME,
        "{calculated_measurements_table}": CalculatedMeasurementsDatabase.MEASUREMENTS_NAME,
    }
