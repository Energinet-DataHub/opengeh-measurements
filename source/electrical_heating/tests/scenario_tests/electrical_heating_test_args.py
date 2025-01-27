from datetime import datetime
from uuid import UUID

from pydantic_settings import BaseSettings


class ElectricalHeatingTestArgs(BaseSettings):
    """Args for testing the electrical heating job."""

    orchestration_instance_id: UUID
    time_zone: str
    catalog_name: str
    calculation_period_start: datetime
    calculation_period_end: datetime

    def __init__(self, env_file_path: str) -> None:
        super().__init__(_env_file=env_file_path)
