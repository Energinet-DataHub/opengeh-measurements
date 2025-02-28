from pydantic import Field
from pydantic_settings import BaseSettings


class SilverSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    silver_container_name (str): The name of the Silver container created in infrastructure.
    silver_database_name (str): The name of the Silver database created in infrastructure.
    """

    silver_container_name: str = Field(init=False)
    silver_database_name: str = Field(init=False)

    class Config:
        case_sensitive = False
