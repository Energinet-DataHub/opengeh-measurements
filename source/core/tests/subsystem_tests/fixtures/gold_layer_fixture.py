from geh_common.databricks import DatabricksApiClient

from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


class GoldLayerFixture:
    def __init__(self) -> None:
        self.gold_settings = GoldSettings()
        self.gold_table = f"{self.gold_settings.gold_database_name}.{GoldTableNames.gold_measurements}"
        self.databricks_settings = DatabricksSettings()  # type: ignore

        print("DEBUG: DatabricksSettings loaded:")
        print(f"DATABRICKS_WORKSPACE_URL: {self.databricks_settings.workspace_url}")
        print(f"DATABRICKS_TOKEN: {self.databricks_settings.token}")
        print(f"DATABRICKS_WAREHOUSE_ID: {self.databricks_settings.warehouse_id}")

        self.databricks_api_client = DatabricksApiClient(
            self.databricks_settings.token,
            self.databricks_settings.workspace_url,
        )

    def assert_measurement_persisted(self, orchestration_instance_id: str) -> None:
        """
        Asserts that a measurement with the given orchestration_instance_id exists in the Gold Layer.

        Args:
            orchestration_instance_id (str): The orchestration instance ID to look for.

        Raises:
            AssertionError: If the measurement does not exist.
        """
        query = f"""
            SELECT COUNT(*) AS count
            FROM {self.gold_table}
            WHERE orchestration_instance_id = '{orchestration_instance_id}'
        """
        result = self.databricks_api_client.execute_statement(
            warehouse_id=self.databricks_settings.warehouse_id, statement=query
        )

        # Extract the count from the result
        count = result[0]["count"] if result else 0
        assert count > 0, f"No measurement found with orchestration_instance_id: {orchestration_instance_id}"
