from core.bronze.infrastructure.config import BronzeTableNames
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.settings.catalog_settings import CatalogSettings
from core.silver.infrastructure.config import SilverTableNames


def substitutions() -> dict[str, str]:
    catalog_settings = CatalogSettings()  # type: ignore
    return {
        "{bronze_database}": catalog_settings.bronze_database_name,
        "{bronze_measurements_table}": BronzeTableNames.bronze_measurements_table,
        "{bronze_submitted_transactions_table}": BronzeTableNames.bronze_submitted_transactions_table,
        "{silver_database}": catalog_settings.silver_database_name,
        "{silver_measurements_table}": SilverTableNames.silver_measurements,
        "{gold_database}": catalog_settings.gold_database_name,
        "{gold_measurements}": GoldTableNames.gold_measurements,
        "{gold_eletrical_heating_view_v1}": GoldViewNames.electrical_heating_view_v1,
    }
