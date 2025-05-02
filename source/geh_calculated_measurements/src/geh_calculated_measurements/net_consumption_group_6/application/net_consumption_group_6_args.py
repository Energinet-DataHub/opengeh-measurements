from datetime import UTC, datetime
from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class NetConsumptionGroup6Args(ApplicationSettings):
    """Args for net consumption group six job."""

    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now(UTC)
    catalog_name: str = Field(init=False)
