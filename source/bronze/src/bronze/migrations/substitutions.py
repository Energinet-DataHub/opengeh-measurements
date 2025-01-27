from bronze.infrastructure.config.database_names import DatabaseNames
from bronze.infrastructure.config.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{bronze_database}": DatabaseNames.bronze_database,
        "{bronze_measurements_table}": TableNames.bronze_measurements_table,
    }
