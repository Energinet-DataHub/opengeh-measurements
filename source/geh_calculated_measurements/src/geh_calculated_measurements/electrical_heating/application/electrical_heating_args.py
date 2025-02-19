from uuid import UUID

from pydantic_settings import BaseSettings


class ElectricalHeatingArgs(BaseSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"

    # databricks location
    catalog_name: str
    schema_name: str

    # databricks tables
    time_series_points_table: str
    consumption_points_table: str
    child_points_table: str
