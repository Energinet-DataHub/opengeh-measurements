from core.gold.infrastructure.config import GoldDatabaseNames, GoldTableNames
from core.silver.infrastructure.config import SilverDatabaseNames, SilverTableNames


def substitutions() -> dict[str, str]:
    return {
        "{silver_database}": SilverDatabaseNames.silver,
        "{silver_measurements_table}": SilverTableNames.silver_measurements,
        "{gold_database}": GoldDatabaseNames.gold,
        "{gold_measurements}": GoldTableNames.gold_measurements,
    }
