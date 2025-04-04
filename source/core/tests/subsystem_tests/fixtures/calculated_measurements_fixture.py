from geh_common.databricks.databricks_api_client import DatabricksApiClient

from tests.subsystem_tests.builders.calculated_measurements_row_builder import CalculatedMeasurementsRow
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


class CalculatedMeasurementsFixture:
    def __init__(self) -> None:
        self.databricks_settings = DatabricksSettings()  # type: ignore

        self.databricks_api_client = DatabricksApiClient(
            self.databricks_settings.token,
            self.databricks_settings.workspace_url,
        )

    def insert_calculated_measurements(self, row: CalculatedMeasurementsRow) -> None:
        """
        Inserts the calculated measurements into the databricks table.
        """
        database_name = "measurements_calculated_internal"
        table_name = "calculated_measurements"

        query = f"""
          INSERT INTO {self.databricks_settings.catalog_name}.{database_name}.{table_name} (
            orchestration_type,
            orchestration_instance_id,
            metering_point_id,
            transaction_id,
            transaction_creation_datetime,
            metering_point_type,
            observation_time,
            quantity
          ) 
          VALUE (
            {row.orchestration_type},
            {row.orchestration_instance_id},
            {row.metering_point_id},
            {row.transaction_id},
            {row.transaction_creation_datetime},
            {row.metering_point_type},
            {row.observation_time},
            {row.quantity}
          )
        """

        self.databricks_api_client.execute_statement(self.databricks_settings.warehouse_id, query)
