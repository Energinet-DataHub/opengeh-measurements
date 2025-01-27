import os

from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import bronze.migrations.substitutions as substitutions
from bronze.domain.constants.database_names import DatabaseNames
from bronze.domain.constants.table_names import TableNames


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = os.environ["CATALOG_NAME"]

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.bronze_database,
        migration_table_name=TableNames.executed_migrations_table,
        migration_scripts_folder_path="bronze.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
