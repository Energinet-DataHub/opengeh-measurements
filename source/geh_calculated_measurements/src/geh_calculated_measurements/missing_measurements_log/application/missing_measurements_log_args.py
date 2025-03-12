from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class MissingMeasurementsLogArgs(ApplicationSettings):
    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    catalog_name: str = Field(init=False)
