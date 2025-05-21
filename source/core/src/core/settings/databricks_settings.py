from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabricksSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Attributes:
    databricks_workspace_url (str): The URL of the Databricks workspace.
    databricks_token (str): The token used to authenticate with the Databricks
    databricks_jobs (str): The list of jobs in Databricks
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    databricks_workspace_url: str = Field(init=False)
    databricks_token: str = Field(init=False)
    databricks_jobs: str = Field(init=False)
