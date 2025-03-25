import uuid
from dataclasses import dataclass
from datetime import datetime

from geh_common.databricks import DatabricksApiClient
from geh_common.domain.types import MeteringPointType

from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@dataclass
class GoldTableRow:
    metering_point_id: str
    orchestration_type: str = "submitted"
    orchestration_instance_id: uuid.UUID = uuid.uuid4()
    observation_time: datetime = datetime(2025, 1, 1, 23, 0, 0)
    quantity: float = 1.7
    quality: str = "measured"
    metering_point_type: MeteringPointType = MeteringPointType.CONSUMPTION
    transaction_id: uuid.UUID = uuid.uuid4()


class GoldTableSeeder:
    def __init__(self, environment_configuration: EnvironmentConfiguration) -> None:
        self.fully_qualified_table_name = f"{environment_configuration.catalog_name}.measurements_gold.measurements"
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.warehouse_id = environment_configuration.warehouse_id

    def _get_statement(self, rows: list[GoldTableRow]) -> str:
        values = ",\n".join(
            [
                f"('{row.metering_point_id}', '{row.orchestration_type}', '{str(row.orchestration_instance_id)}', "
                f"'{row.observation_time.strftime('%Y-%m-%d %H:%M:%S')}', {format(row.quantity, '.3f')}, '{row.quality}', "
                f"'{row.metering_point_type.value}', 'kWh', 'PT1H', '{str(row.transaction_id)}', false, GETDATE(), GETDATE(), GETDATE())"
                for row in rows
            ]
        )
        return f"""
            INSERT INTO {self.fully_qualified_table_name} (
                metering_point_id,
                orchestration_type,
                orchestration_instance_id,
                observation_time,
                quantity,
                quality,
                metering_point_type,
                unit, 
                resolution,
                transaction_id,
                transaction_creation_datetime,
                is_cancelled,
                created,
                modified
            )
            VALUES
            {values}
        """

    def seed(self, rows: GoldTableRow | list[GoldTableRow]) -> None:
        if isinstance(rows, GoldTableRow):
            rows = [rows]

        self.databricks_api_client.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=self._get_statement(rows),
        )
