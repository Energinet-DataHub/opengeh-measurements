from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class CapacitySettlementArgs(ApplicationSettings):
    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int = Field(init=False)
    calculation_year: int = Field(init=False)
    catalog_name: str = Field(init=False)
    electricity_market_data_path: str = Field(init=False)
