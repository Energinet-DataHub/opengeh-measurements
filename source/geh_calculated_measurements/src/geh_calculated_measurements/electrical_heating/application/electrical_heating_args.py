from datetime import datetime
from uuid import UUID

from geh_common.parsing.pydantic_settings_parsing import PydanticParsingSettings


class ElectricalHeatingArgs(PydanticParsingSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now()
    catalog_name: str
    electricity_market_data_path: str
