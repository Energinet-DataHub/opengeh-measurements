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
        self.fully_qualified_table_name = f"{environment_configuration.catalog_name}.electricity_market_measurements_input.net_consumption_group_6_child_metering_point_periods_v1"
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.warehouse_id = environment_configuration.warehouse_id

    def _get_statement(self, rows: list[ChildTableRow]) -> str:
        values = ",\n".join(
            [
                f"('{row.metering_point_id}', '{row.metering_type}', '{str(row.parent_metering_point_id)}', "
                f"'{row.coupled_date.strftime('%Y-%m-%d %H:%M:%S')}', '{row.uncoupled_date.strftime('%Y-%m-%d %H:%M:%S')}')"
                for row in rows
            ]
        )
        return f"""
            INSERT INTO {self.fully_qualified_table_name} (
                metering_point_id,
                metering_type,
                parent_metering_point_id,
                coupled_date,
                uncoupled_date
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
