from pydantic import Field
from pydantic_settings import BaseSettings


class CalculatedSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    calculated_database_name (str): The name of the Calculated database created in infrastructure.
    """

    calculated_database_name: str = Field(init=False)

    class Config:
        case_sensitive = False
