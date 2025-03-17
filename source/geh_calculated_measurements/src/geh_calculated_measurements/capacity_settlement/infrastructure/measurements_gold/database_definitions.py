from pydantic import Field
from pydantic_settings import BaseSettings


class MeasurementsGoldDatabaseDefinition(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    database_name (str): The name of the unity catalog created in infrastructure.
    """

    DATABASE_MEASUREMENTS_GOLDS: str = Field(init=False)

    MEASUREMENTS: str = "capacity_settlement_v1"

    # DATABASE_NAME = "DATABASE_MEASUREMENTS_GOLDS : measurements_gold"
