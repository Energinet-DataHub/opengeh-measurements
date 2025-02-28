from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)

import core.migrations.substitutions as substitutions
from core.migrations.database_names import DatabaseNames
from core.migrations.table_names import TableNames
from core.settings.catalog_settings import CatalogSettings
from core.settings.databricks_settings import DatabricksSettings


def migrate() -> None:
    spark_config = _configure_spark_sql_migrations()

    databricks_settings = DatabricksSettings()
    print(f"Databricks jobs: {databricks_settings.databricks_jobs}")  # noqa: T201
    jobs = databricks_settings.databricks_jobs.split(",")
    for job in jobs:
        print(job)  # noqa: T201

    migration_pipeline.migrate(spark_config)


def _configure_spark_sql_migrations() -> SparkSqlMigrationsConfiguration:
    substitution_variables = substitutions.substitutions()
    catalog_name = CatalogSettings().catalog_name

    return SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.measurements_internal_database,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="core.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )
