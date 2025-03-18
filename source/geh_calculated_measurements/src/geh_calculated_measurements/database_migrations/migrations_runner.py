from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)

import geh_calculated_measurements.database_migrations.substitutions as substitutions
from geh_calculated_measurements.database_migrations.database_definitions import (
    MeasurementsCalculatedInternalDatabaseDefinition,
)
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


def migrate() -> None:
    spark_sql_migrations_configuration = _configure_spark_sql_migrations()
    migration_pipeline.migrate(spark_sql_migrations_configuration)


def _configure_spark_sql_migrations() -> SparkSqlMigrationsConfiguration:
    substitution_variables = substitutions.substitutions()
    catalog_name = CatalogSettings().catalog_name  # type: ignore

    return SparkSqlMigrationsConfiguration(
        migration_schema_name=MeasurementsCalculatedInternalDatabaseDefinition.DATABASE_MEASUREMENTS_CALCULATED_INTERNAL,
        migration_table_name=MeasurementsCalculatedInternalDatabaseDefinition.executed_migrations_table_name,
        migration_scripts_folder_path="geh_calculated_measurements.database_migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )
