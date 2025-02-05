from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.settings.bronze_database_settings import BronzeDatabaseSettings


def substitutions() -> dict[str, str]:
    bronze_database_settings = BronzeDatabaseSettings()  # type: ignore

    return {
        "{bronze_database}": bronze_database_settings.bronze_database_name,
        "{bronze_measurements_table}": TableNames.bronze_measurements_table,
        "{bronze_submitted_transactions_table}": TableNames.bronze_submitted_transactions_table,
    }
