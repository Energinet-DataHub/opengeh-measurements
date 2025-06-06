from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SAPStreamingSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    feature_flag_stream_sap_series (str): The feature flag for streaming into the SAP series table.
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    stream_submitted_to_sap_series: bool = Field(init=False)
