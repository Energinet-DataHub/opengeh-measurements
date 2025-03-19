from datetime import datetime
from typing import Annotated
from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field
from pydantic_settings import NoDecode


class MissingMeasurementsLogArgs(ApplicationSettings):
    orchestration_instance_id: UUID = Field(init=False)
    period_start_datetime: datetime = Field(init=False)
    period_end_datetime: datetime = Field(init=False)
    grid_area_codes: Annotated[list[str], NoDecode] | None = Field(init=False, default=None)
    time_zone: str = "Europe/Copenhagen"
    catalog_name: str = Field(init=False)
