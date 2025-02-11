import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple, Type

from geh_common.parsing.pydantic_settings_parsing import PydanticParsingSettings
from pydantic import Field


class ElectricalHeatingArgs(PydanticParsingSettings):
    """ElectricalHeatingArgs to retrieve and validate parameters and environment variables."""

    catalog_name: str
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now()
    orchestration_instance_id: uuid.UUID
    electricity_market_data_path: str
