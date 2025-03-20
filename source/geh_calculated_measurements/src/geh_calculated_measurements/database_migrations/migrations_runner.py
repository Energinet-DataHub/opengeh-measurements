from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)
from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging

import geh_calculated_measurements.database_migrations.substitutions as substitutions
from geh_calculated_measurements.database_migrations.database_definitions import (
    MeasurementsCalculatedInternalDatabaseDefinition,
)
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings


@start_trace()
def migrate() -> None:
    log = Logger(__name__)
    log_settings = LoggingSettings(subsystem="measurements", cloud_role_name="dbr-calculated-measurements")
    configure_logging(logging_settings=log_settings)

    substitution_variables = substitutions.substitutions()
    catalog_settings = CatalogSettings()
    catalog_name = catalog_settings.catalog_name  # type: ignore

    log.info(f"Initializing migrations with CatalogSettings: {catalog_settings}")
    log.info(f"Initializing migrations with LoggingSettings: {log_settings}")
    spark_sql_migrations_configuration = SparkSqlMigrationsConfiguration(
        migration_schema_name=MeasurementsCalculatedInternalDatabaseDefinition.measurements_calculated_internal_database,
        migration_table_name=MeasurementsCalculatedInternalDatabaseDefinition.executed_migrations_table_name,
        migration_scripts_folder_path="geh_calculated_measurements.database_migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )
    migration_pipeline.migrate(spark_sql_migrations_configuration)
