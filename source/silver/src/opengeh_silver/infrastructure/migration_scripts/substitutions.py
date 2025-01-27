from opengeh_silver.domain.constants.database_names import DatabaseNames
from opengeh_silver.domain.constants.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{silver_database}": DatabaseNames.silver_database,
        "{silver_measurements_table}": TableNames.silver_measurements_table,
    }
