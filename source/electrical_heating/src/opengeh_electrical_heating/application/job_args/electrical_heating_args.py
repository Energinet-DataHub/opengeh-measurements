from dataclasses import dataclass
from uuid import UUID
from pydantic_settings import BaseSettings

@dataclass
class ElectricalHeatingArgs:
    """Args for the electrical heating job."""

    catalog_name: str
    orchestration_instance_id: UUID
    time_zone: str
    electricity_market_data_path: str
