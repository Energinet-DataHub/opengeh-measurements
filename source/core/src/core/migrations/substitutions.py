from src.core.silver.infrastructure.config.database_names import DatabaseNames
from src.core.silver.infrastructure.config.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{silver_database}": DatabaseNames.silver,
        "{silver_measurements_table}": TableNames.silver_measurements,
    }
