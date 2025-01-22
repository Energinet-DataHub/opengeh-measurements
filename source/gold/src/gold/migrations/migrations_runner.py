import os

from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import gold.migrations.migration_scripts.substitutions as substitutions
from gold.infrastructure.config.database_names import DatabaseNames
from gold.infrastructure.config.table_names import TableNames


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = os.environ["CATALOG_NAME"]

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.gold_database,
        migration_table_name=TableNames.executed_migrations_table,
        migration_scripts_folder_path="gold.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
