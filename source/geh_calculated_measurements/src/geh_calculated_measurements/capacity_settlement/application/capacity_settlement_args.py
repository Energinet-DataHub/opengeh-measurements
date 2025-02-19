from uuid import UUID

from geh_common.application.pydantic_settings_parsing import ApplicationSettings


class CapacitySettlementArgs(ApplicationSettings):
    orchestration_instance_id: UUID
    time_zone: str = "Europe/Copenhagen"
    calculation_month: int
    calculation_year: int
