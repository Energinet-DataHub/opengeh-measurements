from pydantic_settings import BaseSettings


class BronzeSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    bronze_container_name (str): The name of the Bronze container created in infrastructure.
    bronze_database_name (str): The name of the Bronze database created in infrastructure.
    """

    bronze_container_name: str
    bronze_database_name: str

    class Config:
        case_sensitive = False
