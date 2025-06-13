from enum import Enum


class QueryNames(Enum):
    SILVER_TO_GOLD = "measurements_silver_to_gold"
    CALCULATED_TO_GOLD = "measurements_calculated_to_gold"
    MIGRATIONS_TO_GOLD = "measurements_migrations_to_gold"

    SILVER_TO_GOLD_SAP_SERIES = "measurements_silver_to_gold_sap_series"
    CALCULATED_TO_GOLD_SAP_SERIES = "measurements_calculated_to_gold_sap_series"
    MIGRATIONS_TO_SAP_SERIES_GOLD = "measurements_migrations_sap_series_to_gold"
