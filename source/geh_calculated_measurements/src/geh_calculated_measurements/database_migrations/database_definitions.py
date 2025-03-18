from pydantic import Field
from pydantic_settings import BaseSettings


class MeasurementsCalculatedInternalDatabaseDefinition(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    database_name (str): The name of the unity catalog created in infrastructure.
    """

    DATABASE_MEASUREMENTS_CALCULATED_INTERNAL: str = Field(init=False)

    executed_migrations_table_name: str = Field(init=False)  # executed_migrations
