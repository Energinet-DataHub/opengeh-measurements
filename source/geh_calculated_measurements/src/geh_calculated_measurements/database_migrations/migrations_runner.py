from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)
from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger

import geh_calculated_measurements.database_migrations.substitutions as substitutions
from geh_calculated_measurements.database_migrations import DatabaseNames
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings

EXECUTED_MIGRATIONS_TABLE_NAME = "executed_migrations"


@start_trace()
def migrate() -> None:
    log = Logger(__name__)
    catalog_settings = CatalogSettings()
    substitution_variables = substitutions.substitutions(catalog_settings.catalog_name)
    log.info(
        f"Initializing migrations with:\nCatalog Settings: {catalog_settings}\nSubstitution Variables: {substitution_variables}"
    )
    _migrate(catalog_settings.catalog_name, substitution_variables)


def _migrate(catalog_name: str, substitution_variables: dict[str, str] | None = None) -> None:
    """Test friendly part of the migration functionality."""
    substitution_variables = substitution_variables or substitutions.substitutions(catalog_name)

    spark_sql_migrations_configuration = SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL,
        migration_table_name=EXECUTED_MIGRATIONS_TABLE_NAME,
        migration_scripts_folder_path="geh_calculated_measurements.database_migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    migration_pipeline.migrate(spark_sql_migrations_configuration)
