from enum import Enum


class QueryNames(Enum):
    SILVER_TO_GOLD = "measurements_silver_to_gold"
    CALCULATED_TO_GOLD = "measurements_calculated_to_gold"
    MIGRATIONS_TO_GOLD = "measurements_migrations_to_gold"
    MIGRATIONS_TO_SAP_SERIES_GOLD = "measurements_migrations_sap_series_to_gold"
