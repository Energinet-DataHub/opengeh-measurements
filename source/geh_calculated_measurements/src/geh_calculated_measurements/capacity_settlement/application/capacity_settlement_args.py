from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings


class CapacitySettlementArgs(BaseSettings, cli_parse_args=True):
    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int = Field(init=False)
    calculation_year: int = Field(init=False)
    catalog_name: str = Field(init=False)
