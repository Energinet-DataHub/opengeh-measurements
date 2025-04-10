from datetime import UTC, datetime
from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class ElectricalHeatingArgs(ApplicationSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now(UTC)
    catalog_name: str = Field(init=False)
    electricity_market_data_path: str = Field(init=False)
