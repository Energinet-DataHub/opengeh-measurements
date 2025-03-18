from pydantic import Field
from pydantic_settings import BaseSettings


class SubmittedTransactionsStreamSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for configuring streaming transactions data.

    Attributes:
    continuous_streaming_enabled (bool): Indicates whether the continuous streaming is enabled. If false, the stream will stop when no more events are available.
    """

    continuous_streaming_enabled: bool = Field(init=False)

    class Config:
        case_sensitive = False
