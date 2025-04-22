from geh_common.databricks.databricks_api_client import DatabricksApiClient
from geh_common.migrations import (
    SparkSqlMigrationsConfiguration,
    migration_pipeline,
)

import core.migrations.substitutions as substitutions
from core.migrations.database_names import DatabaseNames
from core.migrations.table_names import TableNames
from core.settings.catalog_settings import CatalogSettings
from core.settings.databricks_settings import DatabricksSettings


def migrate() -> None:
    spark_config = _configure_spark_sql_migrations()

    _stop_job_runs()
    migration_pipeline.migrate(spark_config)


def _configure_spark_sql_migrations() -> SparkSqlMigrationsConfiguration:
    substitution_variables = substitutions.substitutions()
    catalog_name = CatalogSettings().catalog_name

    return SparkSqlMigrationsConfiguration(
        migration_schema_name=DatabaseNames.measurements_internal_database,
        migration_table_name=TableNames.executed_migrations,
        migration_scripts_folder_path="core.migrations.migration_scripts",
        substitution_variables=substitution_variables,
        catalog_name=catalog_name,
    )


def _stop_job_runs() -> None:
    databricks_settings = DatabricksSettings()
    databricks_api_client = DatabricksApiClient(
        databricks_host=databricks_settings.databricks_workspace_url,
        databricks_token=databricks_settings.databricks_token,
    )

    jobs = databricks_settings.databricks_jobs.split(",")
    for job in jobs:
        print(f"Finding job run for job: {job}")  # noqa: T201
        job_id = databricks_api_client.get_job_id(job)
        run_id = databricks_api_client.get_latest_job_run_id(job_id).run_id

        if run_id is None:
            print(f"No job runs found for job {job}")  # noqa: T201
            continue

        print(f"Canceling job run {run_id} for job {job}")  # noqa: T201
        databricks_api_client.cancel_job_run(run_id)
