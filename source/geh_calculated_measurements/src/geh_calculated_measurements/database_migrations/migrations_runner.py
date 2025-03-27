from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)
from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger

import geh_calculated_measurements.database_migrations.substitutions as substitutions
from geh_calculated_measurements.database_migrations.database_definitions import (
    MeasurementsCalculatedInternalDatabaseDefinition,
)
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


@start_trace()
def migrate() -> None:
    log = Logger(__name__)
    catalog_settings = CatalogSettings()
    substitution_variables = substitutions.substitutions()
    log.info(
        f"Initializing migrations with:\nCatalog Settings: {catalog_settings}\nSubstitution Variables: {substitution_variables}"
    )

    spark_sql_migrations_configuration = SparkSqlMigrationsConfiguration(
        migration_schema_name=MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database,
        migration_table_name=MeasurementsCalculatedInternalDatabaseDefinition.executed_migrations_table_name,
        migration_scripts_folder_path="geh_calculated_measurements.database_migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_settings.catalog_name,
    )
    migration_pipeline.migrate(spark_sql_migrations_configuration)
