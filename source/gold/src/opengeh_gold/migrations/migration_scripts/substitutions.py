from opengeh_gold.infrastructure.config.table_names import TableNames
from opengeh_gold.infrastructure.settings.catalog_settings import CatalogSettings


def substitutions() -> dict[str, str]:
    catalog_settings = CatalogSettings()  # type: ignore

    return {
        "{gold_database}": catalog_settings.gold_database_name,
        "{gold_measurements}": TableNames.gold_measurements,
    }
