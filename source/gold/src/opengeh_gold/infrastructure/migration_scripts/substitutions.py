from opengeh_gold.infrastructure.config.database_names import DatabaseNames
from opengeh_gold.infrastructure.config.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{gold_database}": DatabaseNames.gold_database,
        "{gold_measurements_table}": TableNames.gold_measurements_table,
    }
