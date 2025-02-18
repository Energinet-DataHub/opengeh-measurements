from uuid import UUID

from geh_common.parsing.pydantic_settings_parsing import PydanticParsingSettings


class CapacitySettlementArgs(PydanticParsingSettings):
    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int
    calculation_year: int
