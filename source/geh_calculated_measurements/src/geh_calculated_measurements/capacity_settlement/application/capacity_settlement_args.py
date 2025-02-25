from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings


class CapacitySettlementArgs(BaseSettings):
    _env_file: str | None = Field(default_factory=lambda: None)
    _env_file_encoding: str | None = Field(default_factory=lambda: None)
    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int = Field(init=False)
    calculation_year: int = Field(init=False)
    catalog_name: str = Field()
