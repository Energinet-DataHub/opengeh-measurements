from pydantic import Field
from pydantic_settings import BaseSettings


class DatabricksSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Attributes:
    databricks_workspace_url (str): The URL of the Databricks workspace.
    databricks_token (str): The token used to authenticate with the Databricks
    databricks_jobs (list[str]): The list of jobs in Databricks
    """

    databricks_workspace_url: str = Field(init=False)
    databricks_token: str = Field(init=False)
    databricks_jobs: list[str] = Field(init=False)

    class Config:
        case_sensitive = False
