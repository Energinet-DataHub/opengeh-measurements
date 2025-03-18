from pydantic import Field
from pydantic_settings import BaseSettings


class GoldSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    gold_container_name (str): The name of the Gold container created in infrastructure.
    gold_database_name (str): The name of the Gold database created in infrastructure.
    """

    gold_container_name: str = Field(init=False)
    gold_database_name: str = Field(init=False)

    class Config:
        case_sensitive = False
