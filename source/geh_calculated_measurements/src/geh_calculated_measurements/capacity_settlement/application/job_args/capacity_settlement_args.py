from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings


class CapacitySettlementArgs(BaseSettings):
    time_zone: str = "Europe/Copenhagen"
    orchestration_instance_id: UUID = Field(init=False)
    calculation_month: int = Field(init=False)
    calculation_year: int = Field(init=False)
    catalog_name: str = Field(init=False)
    electricity_market_data_path: str = Field(init=False)
