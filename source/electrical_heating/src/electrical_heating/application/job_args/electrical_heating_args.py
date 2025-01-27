from datetime import datetime
from uuid import UUID

from pydantic_settings import BaseSettings


class ElectricalHeatingArgs(BaseSettings):
    """Args for the electrical heating job."""

    def __init__(
        self,
        orchestration_instance_id: UUID,
        time_zone: str,
        catalog_name: str,
        calculation_period_start: datetime,
        calculation_period_end: datetime,
    ) -> None:
        self.orchestration_instance_id: UUID = orchestration_instance_id
        self.time_zone: str = time_zone
        self.catalog_name: str = catalog_name
        calculation_period_start: datetime = calculation_period_start
        calculation_period_end: datetime = calculation_period_end
