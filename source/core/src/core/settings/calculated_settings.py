from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CalculatedSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    calculated_database_name (str): The name of the Calculated database created in infrastructure.
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    calculated_database_name: str = Field(init=False)
