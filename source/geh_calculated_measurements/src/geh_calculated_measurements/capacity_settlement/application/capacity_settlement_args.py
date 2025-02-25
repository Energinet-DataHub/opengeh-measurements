from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CapacitySettlementArgs(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    _env_file: str | None = None
    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int = Field(init=False)
    calculation_year: int = Field(init=False)
    catalog_name: str = Field()
