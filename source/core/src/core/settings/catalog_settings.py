from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CatalogSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    catalog_name: str = Field(init=False)
