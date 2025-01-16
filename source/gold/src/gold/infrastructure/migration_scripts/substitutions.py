from gold.domain.constants.database_names import DatabaseNames
from gold.domain.constants.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{gold_database}": DatabaseNames.gold_database,
        "{gold_measurements_table}": TableNames.gold_measurements_table,
    }
