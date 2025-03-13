from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class DatabaseNames(ApplicationSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    database_name (str): The name of the unity catalog created in infrastructure.
    """

    measurements_internal_database: str = Field(init=False)

    class Config:
        case_sensitive = False


#class DatabaseNames:
#    # Should probably be an environment variable because it is created in Terraform.
#    # TODO; hardcoded data
#    measurements_internal_database = "measurements_internal"