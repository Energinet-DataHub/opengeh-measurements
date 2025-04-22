from datetime import datetime
from uuid import UUID

from geh_common.application import GridAreaCodes
from geh_common.application.settings import ApplicationSettings
from pydantic import Field

from geh_calculated_measurements.common.infrastructure.electricity_market import (
    DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
)


class MissingMeasurementsLogArgs(ApplicationSettings):
    orchestration_instance_id: UUID = Field(init=False)
    period_start_datetime: datetime = Field(init=False)
    period_end_datetime: datetime = Field(init=False)
    grid_area_codes: GridAreaCodes | None = Field(init=False, default=None)
    time_zone: str = "Europe/Copenhagen"
    catalog_name: str = Field(init=False)
    # For dev enviroments this is overriden by environment variable in dh3environments. It will be a name dedicated
    # for calculated measurements subsystem tests to create a "mock" of the Electricity Market data products.
    electricity_market_database_name: str = Field(
        init=False,
        alias="ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME",
        default=DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
    )
