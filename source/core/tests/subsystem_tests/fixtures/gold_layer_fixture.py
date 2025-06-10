from geh_common.databricks import DatabricksApiClient

from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings
from core.silver.domain.transformations.migrations_transformation import MIGRATION_ORCHESTRATION_INSTANCE_ID
from tests.subsystem_tests.helpers.databricks_assertion_helper import DatabricksAssertionHelper
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


class GoldLayerFixture:
    def __init__(self) -> None:
        self.gold_settings = GoldSettings()
        self.gold_table = f"{self.gold_settings.gold_database_name}.{GoldTableNames.gold_measurements}"
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
                FROM {self.databricks_settings.catalog_name}.{self.gold_table}
                WHERE orchestration_instance_id = '{orchestration_instance_id}'
            """

        DatabricksAssertionHelper().assert_row_persisted(query, timeout, poll_interval)

    def assert_migrated_measurement_persisted(
        self, transaction_id: str, timeout: int = 60, poll_interval: int = 5
    ) -> None:
        query = f"""
                SELECT COUNT(*) AS count
                FROM {self.databricks_settings.catalog_name}.{self.gold_table}
                WHERE orchestration_instance_id = '{MIGRATION_ORCHESTRATION_INSTANCE_ID}'
                AND transaction_id = '{transaction_id}'
            """

        DatabricksAssertionHelper().assert_row_persisted(query, timeout, poll_interval)

    def assert_sap_series_persisted(self, metering_point_id: str, timeout: int = 60, poll_interval: int = 5) -> None:
        query = f"""
                SELECT COUNT(*) AS count
                FROM {self.databricks_settings.catalog_name}.{self.gold_settings.gold_database_name}.{GoldTableNames.gold_measurements_sap_series}
                WHERE metering_point_id = '{metering_point_id}'
            """

        DatabricksAssertionHelper().assert_row_persisted(query, timeout, poll_interval)
