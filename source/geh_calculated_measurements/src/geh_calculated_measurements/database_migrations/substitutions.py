def substitutions() -> dict[str, str]:
    return {
        "{calculated_measurements_database}": CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME,
        "{calculated_measurements_table}": CalculatedMeasurementsDatabaseDefinition.MEASUREMENTS_NAME,
    }
