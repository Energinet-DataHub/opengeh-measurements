import time

from databricks.sdk.service.sql import StatementResponse
from geh_common.databricks import DatabricksApiClient

from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings
from tests.subsystem_tests.settings.catalog_settings import CatalogSettings
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


class GoldLayerFixture:
    def __init__(self) -> None:
        self.gold_settings = GoldSettings()
        self.gold_table = f"{self.gold_settings.gold_database_name}.{GoldTableNames.gold_measurements}"
        self.catalog_settings = CatalogSettings()  # type: ignore
        self.catalog_name = self.catalog_settings.catalog_name
        self.databricks_settings = DatabricksSettings()  # type: ignore
        self.databricks_api_client = DatabricksApiClient(
            self.databricks_settings.token,
            self.databricks_settings.workspace_url,
        )

    def assert_measurement_persisted(
        self, orchestration_instance_id: str, timeout: int = 60, poll_interval: int = 5
    ) -> None:
        query = f"""
                SELECT COUNT(*) AS count
                FROM {self.catalog_name}.{self.gold_table}
                WHERE orchestration_instance_id = '{orchestration_instance_id}'
            """

        start_time = time.time()
        while time.time() - start_time < timeout:
            result = self.databricks_api_client.execute_statement(
                warehouse_id=self.databricks_settings.warehouse_id, statement=query
            )

            count = self._extract_count_from_result(result)

            if count > 0:
                return

            time.sleep(poll_interval)

        raise AssertionError(f"No measurement found with orchestration_instance_id: {orchestration_instance_id}")

    def _extract_count_from_result(self, result: StatementResponse) -> int:
        if not result.result or not result.result.data_array:
            return 0
        count_row = result.result.data_array[0]
        count_value = count_row[0]
        return int(count_value)
