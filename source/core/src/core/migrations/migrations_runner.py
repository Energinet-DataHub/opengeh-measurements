from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import src.migrations.substitutions as substitutions
from src.silver.infrastructure.config.database_names import DatabaseNames
from src.silver.infrastructure.config.table_names import TableNames
from src.silver.infrastructure.helpers.environment_variable_helper import get_catalog_name


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = get_catalog_name()

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.silver,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="opengeh_silver.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
