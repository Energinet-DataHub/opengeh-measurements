from pydantic_settings import BaseSettings, SettingsConfigDict

from tests import PROJECT_ROOT


class EnvironmentConfiguration(BaseSettings):
    databricks_token: str = ""
    databricks_workspace_url: str = ""

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/tests/.env",
        env_file_encoding="utf-8",
    )
