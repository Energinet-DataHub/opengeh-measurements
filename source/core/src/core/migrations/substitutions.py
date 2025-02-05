from core.bronze.domain.constants import BronzeDatabaseNames, BronzeTableNames
from core.gold.infrastructure.config import GoldDatabaseNames, GoldTableNames
from core.silver.infrastructure.config import SilverDatabaseNames, SilverTableNames


def substitutions() -> dict[str, str]:
    return {
        "{bronze_database}": BronzeDatabaseNames.bronze_database,
        "{bronze_measurements_table}": BronzeTableNames.bronze_measurements_table,
        "{bronze_submitted_transactions_table}": BronzeTableNames.bronze_submitted_transactions_table,
        "{silver_database}": SilverDatabaseNames.silver,
        "{silver_measurements_table}": SilverTableNames.silver_measurements,
        "{gold_database}": GoldDatabaseNames.gold,
        "{gold_measurements}": GoldTableNames.gold_measurements,
    }
