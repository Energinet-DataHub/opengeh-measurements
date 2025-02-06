import sys

from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames


def substitutions() -> dict[str, str]:
    return {
        "{bronze_database}": DatabaseNames.bronze_database,
        "{bronze_measurements_table}": TableNames.bronze_measurements_table,
        "{bronze_submitted_transactions_table}": TableNames.bronze_submitted_transactions_table,
        "{bronze_migrated_transactions_table}": TableNames.bronze_migrated_transactions_table,
        "{SKIP_IN_SUBSYSTEM_TEST}": "--" if "unittest" in sys.modules.keys() else "",
    }
