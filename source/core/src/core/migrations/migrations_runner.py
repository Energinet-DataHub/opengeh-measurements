from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)

import core.migrations.substitutions as substitutions
from core.migrations.database_names import DatabaseNames
from core.migrations.table_names import TableNames
from core.settings.catalog_settings import CatalogSettings


def migrate() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = CatalogSettings().catalog_name

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.measurements_internal_database,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="core.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    migration_pipeline.migrate(spark_config)
