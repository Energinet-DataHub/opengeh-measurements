from pydantic_settings import BaseSettings


class CatalogSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    catalog_name (str): The name of the unity catalog created in infrastructure.
    bronze_container_name (str): The name of the Bronze container created in infrastructure.
    silver_container_name (str): The name of the Silver container created in infrastructure.
    gold_container_name (str): The name of the Gold container created in infrastructure.
    bronze_database_name (str): The name of the Bronze database created in infrastructure.
    silver_database_name (str): The name of the Silver database created in infrastructure.
    gold_database_name (str): The name of the Gold database created in infrastructure.
    """

    catalog_name: str
    bronze_container_name: str
    silver_container_name: str
    gold_container_name: str
    bronze_database_name: str
    silver_database_name: str
    gold_database_name: str

    migrations_silver_database_name: str

    class Config:
        case_sensitive = False
