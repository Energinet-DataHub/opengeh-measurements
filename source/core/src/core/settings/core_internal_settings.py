from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CoreInternalSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    core_internal_container_name (str): The name of the Core Internal container created in infrastructure.
    core_internal_database_name (str): The name of the Core Internal database created in infrastructure.
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    core_internal_container_name: str = Field(init=False)
    core_internal_database_name: str = Field(init=False)
