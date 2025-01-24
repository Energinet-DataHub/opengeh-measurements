from silver.infrastructure.silver.database_names import DatabaseNames
from silver.infrastructure.silver.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{silver_database}": DatabaseNames.silver_database,
        "{silver_measurements_table}": TableNames.silver_measurements_table,
    }
