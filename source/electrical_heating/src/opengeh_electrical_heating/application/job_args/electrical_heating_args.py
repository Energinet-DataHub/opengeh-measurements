from uuid import UUID

from pydantic_settings import BaseSettings


class ElectricalHeatingArgs(BaseSettings):
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID
    time_zone: str
    catalog_name: str
