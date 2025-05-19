from geh_common.application.settings import ApplicationSettings
from pydantic import Field


class OptimizeArgs(ApplicationSettings):
    database: str = Field(init=False)
    table: str = Field(init=False)
