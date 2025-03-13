from geh_common.application.settings import ApplicationSettings
from pydantic import Field

#class CalculatedMeasurementsInternalDatabaseDefinition:
#    # note: This database is also hardcoded somewhere else.
#    DATABASE_NAME = "measurements_calculated_internal"
#    # Should match whatever name is currently being used in Databricks
#    MEASUREMENTS_NAME = "calculated_measurements"
#    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME = "capacity_settlement_ten_largest_quantities"
#    CAPACITY_SETTLEMENT_CALCULATIONS_NAME = "capacity_settlement_calculations"



class CalculatedMeasurementsInternalDatabaseDefinition(ApplicationSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    """

    MEASUREMENTS_CALCULATED_INTERNAL_DATABASE: str = Field(init=False)

    MEASUREMENTS_NAME = "calculated_measurements"
    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_NAME = "capacity_settlement_ten_largest_quantities"
    CAPACITY_SETTLEMENT_CALCULATIONS_NAME = "capacity_settlement_calculations"

    class Config:
        case_sensitive = False

#class CalculatedMeasurementsDatabaseDefinition:
#    DATABASE_NAME = "measurements_calculated"

class CalculatedMeasurementsDatabaseDefinition(ApplicationSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    """

    measurements_calculated_database: str = Field(init=False)

    class Config:
        case_sensitive = False