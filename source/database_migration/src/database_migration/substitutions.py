from database_migration.constants.database_constants import DatabaseConstants
from database_migration.constants.table_constants import TableConstants


def substitutions() -> dict[str, str]:
    return {
        "{bronze_database}": DatabaseConstants.bronze_database,
        "{bronze_measurements_table}": TableConstants.bronze_measurements_table,
    }
