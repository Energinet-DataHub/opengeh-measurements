from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import core.migrations.substitutions as substitutions
from src.core.silver.infrastructure.config import SilverDatabaseNames, SilverTableNames
from src.core.silver.infrastructure.helpers.environment_variable_helper import get_catalog_name


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = get_catalog_name()

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=SilverDatabaseNames.silver,
        migration_table_name=SilverTableNames.executed_migrations,
        migration_scripts_folder_path="core.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
