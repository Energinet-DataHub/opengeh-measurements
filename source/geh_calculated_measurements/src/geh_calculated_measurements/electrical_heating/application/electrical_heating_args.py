from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings


class ElectricalHeatingArgs(BaseSettings, cli_parse_args=True):
    """Args for the electrical heating job."""

    time_zone: str = "Europe/Copenhagen"
    orchestration_instance_id: UUID = Field(init=False)
    catalog_name: str = Field(init=False)
    electricity_market_data_path: str = Field(init=False)
