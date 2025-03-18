from typing import ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings


class CalculatedMeasurementsInternalDatabaseDefinition(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.
    """

    DATABASE_MEASUREMENTS_CALCULATED_INTERNAL: str = Field(init=False)
    MEASUREMENTS_NAME: ClassVar[str] = "calculated_measurements"
    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME: ClassVar[str] = "capacity_settlement_ten_largest_quantities"
    CAPACITY_SETTLEMENT_CALCULATIONS_NAME: ClassVar[str] = "capacity_settlement_calculations"


class CalculatedMeasurementsDatabaseDefinition(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.
    """

    measurements_calculated_database: str = Field(init=False)
