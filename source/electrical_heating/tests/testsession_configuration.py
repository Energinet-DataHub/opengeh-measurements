from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from tests import PROJECT_ROOT


class ScenarioTest(BaseModel):
    show_actual_and_expected: bool = False
    show_columns_when_actual_and_expected_are_equal: bool = False
    show_actual_and_expected_count: bool = False
    ignore_extra_columns_in_actual: bool = True


class ContainerTest(BaseModel):
    databricks_token: str = ""
    databricks_workspace_url: str = ""


class TestSessionConfiguration(BaseSettings):
    """The main configuration class for the test session."""

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/tests/testsession.local.settings.env",
        env_file_encoding="utf-8",
        # This is the delimiter used to separate nested keys in environment variables (see the env file).
        env_nested_delimiter="__",
    )

    scenario_test: ScenarioTest = ScenarioTest()
    container_test: ContainerTest = ContainerTest()
