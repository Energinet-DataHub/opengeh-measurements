import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple, Type

from pydantic import Field
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict

DOTENV = os.path.join(os.path.dirname(__file__), ".env")


class ElectricalHeatingArgs(BaseSettings):
    """ElectricalHeatingArgs to retrieve and validate parameters and environment variables automatically.

    Parameters can come from both runtime (CLI) or from environment variables.
    The priority is CLI parameters first and then environment variables.
    """

    catalog_name: str = Field(alias="catalog-name")
    time_zone: str = Field(alias="time-zone", default="Europe/Copenhagen")
    execution_start_datetime: datetime = Field(alias="execution-start-datetime", default=datetime.now(timezone.utc))
    orchestration_instance_id: uuid.UUID = Field(alias="orchestration-instance-id")
    electricity_market_data_path: str = Field(alias="electricity-market-data-path")
    model_config = SettingsConfigDict(env_file=DOTENV, populate_by_name=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            CliSettingsSource(settings_cls, cli_parse_args=True, cli_ignore_unknown_args=True),
            env_settings,
            dotenv_settings,
            init_settings,
        )
