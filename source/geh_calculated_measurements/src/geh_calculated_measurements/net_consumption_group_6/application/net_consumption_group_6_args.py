from datetime import UTC, datetime
from uuid import UUID

from geh_common.application.settings import ApplicationSettings
from pydantic import Field

from geh_calculated_measurements.common.infrastructure.electricity_market import (
    DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
)


class NetConsumptionGroup6Args(ApplicationSettings):
    """Args for net consumption group six job."""

    orchestration_instance_id: UUID = Field(init=False)
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now(UTC)
    catalog_name: str = Field(init=False)
    electricity_market_data_path: str = Field(init=False)
    # For dev enviroments this is overriden by environment variable in dh3environments. It will be a name dedicated
    # for calculated measurements subsystem tests to create a "mock" of the Electricity Market data products.
    electricity_market_database_name: str = Field(
        init=False,
        alias="ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME",
        default=DEFAULT_ELECTRICITY_MARKET_MEASUREMENTS_INPUT_DATABASE_NAME,
    )
