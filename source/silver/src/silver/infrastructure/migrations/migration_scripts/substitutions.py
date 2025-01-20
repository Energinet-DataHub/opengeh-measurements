from source.silver.src.silver.infrastructure.migrations.migration_scripts.database_names import DatabaseNames
from source.silver.src.silver.infrastructure.migrations.migration_scripts.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{silver_database}": DatabaseNames.silver_database,
        "{silver_measurements_table}": TableNames.silver_measurements_table,
    }
