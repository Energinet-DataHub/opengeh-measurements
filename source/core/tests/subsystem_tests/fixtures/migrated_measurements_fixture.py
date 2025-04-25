from geh_common.databricks.databricks_api_client import DatabricksApiClient

from core.bronze.domain.constants.column_names.migrations_silver_time_series_column_names import (
    MigrationsSilverTimeSeriesColumnNames,
)
from core.bronze.infrastructure.config.table_names import MigrationsTableNames
from core.settings.migrations_settings import MigrationsSettings
from tests.subsystem_tests.builders.migrated_measurements_row_builder import MigratedMeasurementsRow
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


class MigratedMeasurementsFixture:
    MIGRATION_TO_MEASUREMENTS_JOB_NAME = "Migrate Transactions From Migrations To Measurements"

    def __init__(self) -> None:
        self.databricks_settings = DatabricksSettings()  # type: ignore

        self.databricks_api_client = DatabricksApiClient(
            self.databricks_settings.token,
            self.databricks_settings.workspace_url,
        )

    def insert_migrated_measurements(self, row: MigratedMeasurementsRow) -> None:
        """
        Inserts the migrated measurements into the databricks table.
        """
        database_name = "migrations_silver"
        table_name = MigrationsTableNames.silver_time_series_table

        values_as_string = ", ".join([f"struct({t[0]}, '{t[1]}', {t[2]})" for t in row.values])

        query = f"""
          INSERT INTO {self.databricks_settings.catalog_name}.{database_name}.{table_name} (
            {MigrationsSilverTimeSeriesColumnNames.metering_point_id},
            {MigrationsSilverTimeSeriesColumnNames.type_of_mp},
            {MigrationsSilverTimeSeriesColumnNames.historical_flag},
            {MigrationsSilverTimeSeriesColumnNames.resolution},
            {MigrationsSilverTimeSeriesColumnNames.transaction_id},
            {MigrationsSilverTimeSeriesColumnNames.transaction_insert_date},
            {MigrationsSilverTimeSeriesColumnNames.unit},
            {MigrationsSilverTimeSeriesColumnNames.status},
            {MigrationsSilverTimeSeriesColumnNames.read_reason},
            {MigrationsSilverTimeSeriesColumnNames.valid_from_date},
            {MigrationsSilverTimeSeriesColumnNames.valid_to_date},
            {MigrationsSilverTimeSeriesColumnNames.values},
            {MigrationsSilverTimeSeriesColumnNames.partitioning_col},
            {MigrationsSilverTimeSeriesColumnNames.created}
          )
          VALUES (
            '{row.metering_point_id}',
            '{row.type_of_mp}',
            '{row.historical_flag}',
            '{row.resolution}',
            '{row.transaction_id}',
            '{row.transaction_insert_date}',
            '{row.unit}',
            '{row.status}',
            '{row.read_reason}',
            '{row.valid_from_date}',
            '{row.valid_to_date}',
            array({values_as_string}),
            '{row.partitioning_col}',
            '{row.created}'
          )
        """

        self.databricks_api_client.execute_statement(self.databricks_settings.warehouse_id, query)

    def start_migrations_to_measurements_job(self) -> None:
        """
        Starts the migration to measurements job and wait for it to finish.
        """
        job_id = self.databricks_api_client.get_job_id(self.MIGRATION_TO_MEASUREMENTS_JOB_NAME)
        run_id = self.databricks_api_client.start_job(job_id)
        self.databricks_api_client.wait_for_job_completion(run_id)
