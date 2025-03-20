from typing import ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings

from geh_calculated_measurements.electrical_heating.domain import time_series_points_v1


class MeasurementsGoldDatabaseDefinition(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.
    """

    DATABASE_MEASUREMENTS_GOLD: str = Field(init=False)
    TIME_SERIES_POINTS_NAME: ClassVar[str] = "electrical_heating_v1"


electrical_heating_v1_schema = time_series_points_v1
