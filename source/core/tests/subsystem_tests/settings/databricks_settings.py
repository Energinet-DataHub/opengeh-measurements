from pydantic_settings import BaseSettings


class DatabricksSettings(BaseSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    workspace_url: str
    token: str

    class Config:
        case_sensitive = False
