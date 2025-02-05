from pydantic_settings import BaseSettings


class BronzeDatabaseSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the Bronze database.

    Attributes:
    bronze_database_name (str): The name of the Bronze database created in infrastructure.
    """

    bronze_database_name: str

    class Config:
        case_sensitive = False
