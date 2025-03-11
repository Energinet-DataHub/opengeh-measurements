from datetime import datetime

from geh_common.databricks import DatabricksApiClient
from geh_common.domain.types import MeteringPointType
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class GoldTableRow:
    metering_point_id: str
    orchestration_type: str = "submitted"
    observation_time: datetime
    quantity: str = "1.700"
    quality: str = "measured"
    metering_point_type: MeteringPointType = MeteringPointType.CONSUMPTION


class GoldTableSeeder:
    def __init__(self, environment_configuration: EnvironmentConfiguration) -> None:
        self.fully_qualified_table_name = f"{environment_configuration.catalog_name}.measurements_gold.measurements"
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.warehouse_id = environment_configuration.warehouse_id

    def _get_statement(self, row: GoldTableRow) -> str:
        return f"""
            INSERT INTO {self.fully_qualified_table_name} (
                metering_point_id,
                orchestration_type,
                observation_time,
                quantity,
                quality,
                metering_point_type,
                transaction_id,
                transaction_creation_datetime,
                created,
                modified
            )
            SELECT
                '{row.metering_point_id}' AS metering_point_id,
                '{row.orchestration_type}' AS orchestration_type,
                '{row.observation_time.strftime}' AS observation_time,
                {row.quantity} AS quantity,
                '{row.quality}' AS quality,
                '{row.metering_point_type.value}' AS metering_point_type,
                REPLACE(CAST(uuid() AS VARCHAR(50)), '-', '') AS transaction_id,
                GETDATE() AS transaction_creation_datetime,
                GETDATE() AS created,
                GETDATE() AS modified
        """

    def seed_gold_table(self, row: GoldTableRow) -> None:
        self.databricks_api_client.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=self._get_statement(row),
        )
