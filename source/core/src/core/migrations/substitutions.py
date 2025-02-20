from core.bronze.infrastructure.config import BronzeTableNames
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames


def substitutions() -> dict[str, str]:
    bronze_settings = BronzeSettings()  # type: ignore
    silver_settings = SilverSettings()  # type: ignore
    gold_settings = GoldSettings()  # type: ignore

    return {
        "{bronze_database}": bronze_settings.bronze_database_name,
        "{bronze_submitted_transactions_table}": BronzeTableNames.bronze_submitted_transactions_table,
        "{silver_database}": silver_settings.silver_database_name,
        "{silver_measurements_table}": SilverTableNames.silver_measurements,
        "{gold_database}": gold_settings.gold_database_name,
        "{gold_measurements}": GoldTableNames.gold_measurements,
        "{gold_electrical_heating_v1}": GoldViewNames.electrical_heating_v1,
        "{gold_capacity_settlement_v1}": GoldViewNames.capacity_settlement_v1,
    }
