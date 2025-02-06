import os

from spark_sql_migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import opengeh_bronze.migrations.substitutions as substitutions
from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.settings import CatalogSettings


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = os.environ["CATALOG_NAME"]
    bronze_database_name = CatalogSettings().bronze_database_name  # type: ignore

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=bronze_database.bronze_database_name,
        migration_table_name=TableNames.executed_migrations_table,
        migration_scripts_folder_path="opengeh_bronze.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
