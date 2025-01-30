from dataclasses import dataclass
from uuid import UUID

from pydantic_settings import BaseSettings


class CapacitySettlementArgs(BaseSettings):
    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int
    calculation_year: int
