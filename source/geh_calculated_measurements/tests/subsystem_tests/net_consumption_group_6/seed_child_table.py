from dataclasses import dataclass
from datetime import datetime

from geh_common.databricks import DatabricksApiClient

from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@dataclass
class ChildTableRow:
    metering_point_id: str
    metering_type: str
    parent_metering_point_id: str
    coupled_date: datetime
    uncoupled_date: datetime


class ChildTableSeeder:
    def __init__(self, environment_configuration: EnvironmentConfiguration) -> None:
        self.fully_qualified_table_name = f"{environment_configuration.catalog_name}.measurements_gold.measurements"
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.warehouse_id = environment_configuration.warehouse_id

    def _get_statement(self, rows: list[ChildTableRow]) -> str:
        values = ",\n".join(
            [
                f"('{row.metering_point_id}', '{row.orchestration_type}', '{str(row.orchestration_instance_id)}', "
                f"'{row.observation_time.strftime('%Y-%m-%d %H:%M:%S')}', {format(row.quantity, '.3f')}, '{row.quality}', "
                f"'{row.metering_point_type.value}', 'kWh', 'PT1H', '{str(row.transaction_id)}', GETDATE(), GETDATE(), GETDATE())"
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
                created,
                modified
            )
            VALUES
            {values}
        """

    def seed(self, rows: ChildTableRow | list[ChildTableRow]) -> None:
        if isinstance(rows, ChildTableRow):
            rows = [rows]

        self.databricks_api_client.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=self._get_statement(rows),
        )
