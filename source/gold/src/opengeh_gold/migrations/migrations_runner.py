from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import opengeh_gold.migrations.migration_scripts.substitutions as substitutions
from opengeh_gold.infrastructure.config.table_names import TableNames
from opengeh_gold.infrastructure.settings.catalog_settings import CatalogSettings


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_settings = CatalogSettings()  # type: ignore

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=catalog_settings.gold_database_name,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="opengeh_gold.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_settings.catalog_name,
    )

    create_and_configure_container(spark_config)
