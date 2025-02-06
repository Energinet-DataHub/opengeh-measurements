from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import opengeh_silver.migrations.substitutions as substitutions
from opengeh_silver.application.streams.calculated_stream import DatabaseSettings
from opengeh_silver.infrastructure.config.table_names import TableNames
from opengeh_silver.infrastructure.helpers.environment_variable_helper import get_catalog_name
from opengeh_silver.infrastructure.settings.database_settings import DatabaseSettings


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = get_catalog_name()
    database_settings = DatabaseSettings()  # type: ignore

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=database_settings.silver_database_name,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="opengeh_silver.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
