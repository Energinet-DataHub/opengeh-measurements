from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.settings.catalog_settings import CatalogSettings


def substitutions() -> dict[str, str]:
    catalog_settings = CatalogSettings()  # type: ignore

    return {
        "{bronze_database}": catalog_settings.bronze_database_name,
        "{bronze_measurements_table}": TableNames.bronze_measurements_table,
        "{bronze_submitted_transactions_table}": TableNames.bronze_submitted_transactions_table,
    }
