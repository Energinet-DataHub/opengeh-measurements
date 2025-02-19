from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    create_and_configure_container,
    migration_pipeline,
)

import geh_calculated_measurements.database_migrations.substitutions as substitutions
from geh_calculated_measurements.database_migrations.database_names import DatabaseNames
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings
from geh_calculated_measurements.database_migrations.table_names import TableNames


def migrate() -> None:
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations() -> None:
    substitution_variables = substitutions.substitutions()
    catalog_name = CatalogSettings().catalog_name  # type: ignore

    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.measurements_calculated_internal_database,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="geh_calculated_measurements.database_migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )

    create_and_configure_container(spark_config)
