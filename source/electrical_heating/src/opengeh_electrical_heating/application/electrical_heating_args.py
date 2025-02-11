import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple, Type

from pydantic import Field
from pydantic_settings import BaseSettings, CliSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict


class ElectricalHeatingArgs(
    BaseSettings,
    cli_parse_args=True,
    cli_kebab_case=True,
    cli_ignore_unknown_args=True,
):
    # TODO: inherit from base pydantic settings to simplify generic setup
    """ElectricalHeatingArgs to retrieve and validate parameters and environment variables automatically.

    Parameters can come from both runtime (CLI) or from environment variables.
    The priority is CLI parameters first and then environment variables.
    """

    catalog_name: str
    time_zone: str = "Europe/Copenhagen"
    execution_start_datetime: datetime = datetime.now()
    orchestration_instance_id: uuid.UUID
    electricity_market_data_path: str

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
            CliSettingsSource(settings_cls),
            env_settings,
            dotenv_settings,
            init_settings,
        )
