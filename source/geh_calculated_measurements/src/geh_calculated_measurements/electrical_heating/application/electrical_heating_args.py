from datetime import datetime
from uuid import UUID

from geh_common.application.settings import ApplicationSettings


class ElectricalHeatingArgs(ApplicationSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID | None = None
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now()
    catalog_name: str
    electricity_market_data_path: str
