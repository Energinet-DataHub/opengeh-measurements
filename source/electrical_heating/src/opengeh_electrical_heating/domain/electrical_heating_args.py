from uuid import UUID

from pydantic_settings import BaseSettings


class ElectricalHeatingArgs(BaseSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"
    catalog_name: str
    electricity_market_data_path: str
