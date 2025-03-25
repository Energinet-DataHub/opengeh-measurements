from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from geh_common.databricks import DatabricksApiClient

from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@dataclass
class ParentTableRow:
    metering_point_id: str
    has_electrical_heating: bool
    settlement_month: int
    period_from_date: datetime
    period_to_date: Optional[datetime]
    move_in: bool


class ParentTableSeeder:
    def __init__(self, environment_configuration: EnvironmentConfiguration) -> None:
        self.fully_qualified_table_name = f"{environment_configuration.catalog_name}.electricity_market_measurements_input.net_consumption_group_6_consumption_metering_point_periods_v1"
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.warehouse_id = environment_configuration.warehouse_id

    def _get_statement(self, rows: list[ParentTableRow]) -> str:
        values = ",\n".join(
            [
                f"('{row.metering_point_id}',{row.has_electrical_heating},{row.settlement_month},"
                f"'{row.period_from_date.strftime('%Y-%m-%d %H:%M:%S')}',"
                + (
                    f"'{row.period_to_date.strftime('%Y-%m-%d %H:%M:%S')}'"
                    if row.period_to_date is not None
                    else "NULL"
                )
                + ", "
                f"{row.move_in})"
                for row in rows
            ]
        )
        return f"""
            INSERT INTO {self.fully_qualified_table_name} (
                metering_point_id,
                has_electrical_heating,
                settlement_month,
                period_from_date,
                period_to_date,
                move_in
            )
            VALUES
            {values}
        """

    def seed(self, rows: ParentTableRow | list[ParentTableRow]) -> None:
        if isinstance(rows, ParentTableRow):
            rows = [rows]

        self.databricks_api_client.execute_statement(
            warehouse_id=self.warehouse_id, statement=self._get_statement(rows)
        )
