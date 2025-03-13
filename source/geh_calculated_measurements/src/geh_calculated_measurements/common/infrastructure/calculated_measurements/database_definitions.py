from pydantic import Field
from pydantic_settings import BaseSettings

class CalculatedMeasurementsInternalDatabaseDefinition(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    """

    MEASUREMENTS_CALCULATED_INTERNAL_DATABASE: str = Field(init=False)
    MEASUREMENTS_NAME = "calculated_measurements"

    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME = "capacity_settlement_ten_largest_quantities"
    CAPACITY_SETTLEMENT_CALCULATIONS_NAME = "capacity_settlement_calculations"

class CalculatedMeasurementsDatabaseDefinition:
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    """

    measurements_calculated_database: str = Field(init=False)
