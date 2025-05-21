from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# These settings are for the Migrations subsystem.
class MigrationsSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    silver_database_name (str): The name of the silver Migrations database that we should use.
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    silver_database_name: str = Field(init=False)
