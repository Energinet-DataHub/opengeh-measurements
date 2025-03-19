from datetime import datetime
from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class MissingMeasurementsLogArgs(ApplicationSettings):
    orchestration_instance_id: UUID = Field(init=False)
    period_start_datetime: datetime = Field(init=False)
    period_end_datetime: datetime = Field(init=False)
    grid_area_codes: list[str] | None = Field(default=None)
    time_zone: str = "Europe/Copenhagen"
    catalog_name: str = Field(init=False)
