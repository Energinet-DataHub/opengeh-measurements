import uuid

from geh_common.parsing.pydantic_settings_parsing import PydanticParsingSettings


class CapacitySettlementArgs(PydanticParsingSettings):
    """Base settings to contain run parameters for the job."""

    orchestration_instance_id: uuid.UUID
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int
    calculation_year: int
