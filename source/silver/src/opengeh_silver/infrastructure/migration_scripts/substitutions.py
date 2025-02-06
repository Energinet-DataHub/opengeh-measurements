from opengeh_silver.domain.constants.table_names import TableNames
from opengeh_silver.infrastructure.settings.catalog_settings import CatalogSettings


def substitutions() -> dict[str, str]:
    catalog_settings = CatalogSettings()  # type: ignore

    return {
        "{silver_database}": catalog_settings.silver_database_name,
        "{silver_measurements_table}": TableNames.silver_measurements_table,
    }
