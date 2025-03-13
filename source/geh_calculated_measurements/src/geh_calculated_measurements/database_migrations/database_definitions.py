from geh_common.application.settings import ApplicationSettings
from pydantic import Field

#class MeasurementsCalculatedInternalDatabaseDefinition:
#    # TODO; hardcoded data
#    measurements_calculated_internal_database = "measurements_calculated_internal"
#    executed_migrations_table_name = "executed_migrations"
#    MEASUREMENTS_NAME = "measurements"    NOTE: This is not being used


class MeasurementsCalculatedInternalDatabaseDefinition(ApplicationSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    database_name (str): The name of the unity catalog created in infrastructure.
    """

    measurements_calculated_internal_database: str = Field(init=False)

    MEASUREMENTS_NAME: str = Field(init=False)
    executed_migrations_table_name: str = Field(init=False)

    class Config:
        case_sensitive = False