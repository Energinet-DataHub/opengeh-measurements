from core.bronze.infrastructure.config import BronzeTableNames
from core.gold.infrastructure.config import GoldTableNames, GoldViewNames
from core.receipts.infrastructure.config.table_names import CoreInternalTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.core_internal_settings import CoreInternalSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames


def substitutions() -> dict[str, str]:
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()
    gold_settings = GoldSettings()
    core_internal_settings = CoreInternalSettings()

    return {
        "{bronze_database}": bronze_settings.bronze_database_name,
        "{bronze_submitted_transactions_table}": BronzeTableNames.bronze_submitted_transactions_table,
        "{bronze_invalid_submitted_transactions_table}": BronzeTableNames.bronze_invalid_submitted_transactions,
        "{silver_database}": silver_settings.silver_database_name,
        "{bronze_migrated_transactions_table}": BronzeTableNames.bronze_migrated_transactions_table,
        "{silver_measurements_table}": SilverTableNames.silver_measurements,
        "{submitted_transactions_quarantined_table}": BronzeTableNames.bronze_submitted_transactions_quarantined,
        "{gold_database}": gold_settings.gold_database_name,
        "{gold_measurements}": GoldTableNames.gold_measurements,
        "{gold_measurements_series_sap}": GoldTableNames.gold_measurements_series_sap,
        "{gold_electrical_heating_v1}": GoldViewNames.electrical_heating_v1,
        "{gold_capacity_settlement_v1}": GoldViewNames.capacity_settlement_v1,
        "{gold_current_v1}": GoldViewNames.current_v1,
        "{core_internal_database}": core_internal_settings.core_internal_database_name,
        "{process_manager_receipts}": CoreInternalTableNames.process_manager_receipts,
        "{gold_sap_delta_v1}": GoldViewNames.sap_delta_v1,
    }
