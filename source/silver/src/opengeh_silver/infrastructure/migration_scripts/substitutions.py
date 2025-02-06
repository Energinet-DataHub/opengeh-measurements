from opengeh_silver.domain.constants.table_names import TableNames
from opengeh_silver.infrastructure.settings.database_settings import DatabaseSettings


def substitutions() -> dict[str, str]:
    database_settings = DatabaseSettings()  # type: ignore

    return {
        "{silver_database}": database_settings.silver_database_name,
        "{silver_measurements_table}": TableNames.silver_measurements_table,
    }
