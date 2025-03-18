from pydantic import Field
from pydantic_settings import BaseSettings


class CatalogSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    """

    catalog_name: str = Field(init=False)

    class Config:
        case_sensitive = False
