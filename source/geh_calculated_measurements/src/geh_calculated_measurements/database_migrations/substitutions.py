from geh_calculated_measurements.database_migrations import DatabaseNames


def substitutions(catalog_name: str) -> dict[str, str]:
    return {
        "{calculated_measurements_internal_database}": DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL,
        "{calculated_measurements_database}": DatabaseNames.MEASUREMENTS_CALCULATED,
        "{catalog_name}": catalog_name,
    }
