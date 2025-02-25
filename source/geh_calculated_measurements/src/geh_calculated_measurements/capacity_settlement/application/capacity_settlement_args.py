from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings


class CapacitySettlementArgs(BaseSettings):
    orchestration_instance_id: UUID = Field()
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int = Field()
    calculation_year: int = Field()
    catalog_name: str = Field()
