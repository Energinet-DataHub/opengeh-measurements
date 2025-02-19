from typing import Optional
from uuid import UUID

from pydantic_settings import BaseSettings


class ElectricalHeatingArgs(BaseSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"
    electricity_market_data_path: Optional[str] = None

    # databricks location
    catalog_name: Optional[str] = None
    schema_name: Optional[str] = None

    # databricks tables
    time_series_points_table: Optional[str] = None
    consumption_points_table: Optional[str] = None
    child_points_table: Optional[str] = None
