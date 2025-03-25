from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from geh_common.databricks import DatabricksApiClient
from geh_common.domain.types import MeteringPointResolution

from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


@dataclass
class MeteringPointPeriodsRow:
    metering_point_id: str
    grid_area_code: str
    period_from_date: datetime
    period_to_date: Optional[datetime]
    resolution: MeteringPointResolution = MeteringPointResolution.HOUR


class MeteringPointPeriodsTableSeeder:
    def __init__(self, environment_configuration: EnvironmentConfiguration) -> None:
        self.fully_qualified_table_name = f"{environment_configuration.catalog_name}.electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1"
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        self.warehouse_id = environment_configuration.warehouse_id

    def _get_statement(self, rows: list[MeteringPointPeriodsRow]) -> str:
        values = ",\n".join(
            [
                f"('{row.metering_point_id}',{row.grid_area_code},{row.resolution.value},"
                f"'{row.period_from_date.strftime('%Y-%m-%d %H:%M:%S')}',"
                + (
                    f"'{row.period_to_date.strftime('%Y-%m-%d %H:%M:%S')}'"
                    if row.period_to_date is not None
                    else "NULL"
                )
                + ", "
                for row in rows
            ]
        )

        return f"""
            INSERT INTO {self.fully_qualified_table_name} (
                metering_point_id,
                grid_area_code,
                resolution,
                period_from_date,
                period_to_date,
            )
            VALUES
            {values}
        """

    def seed(self, rows: MeteringPointPeriodsRow | list[MeteringPointPeriodsRow]) -> None:
        if isinstance(rows, MeteringPointPeriodsRow):
            rows = [rows]

        self.databricks_api_client.execute_statement(
            warehouse_id=self.warehouse_id, statement=self._get_statement(rows)
        )
