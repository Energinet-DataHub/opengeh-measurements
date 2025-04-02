from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings
from pyspark.sql.streaming.readwriter import DataStreamWriter


class StreamingSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for the unity catalog used by package.

    Attributes:
    continuous_streaming_enabled (bool): Indicates whether the continuous streaming is enabled. If false, the stream will stop when no more events are available.
    """

    continuous_streaming_enabled: bool = Field(init=False)
    maxFilesPerTrigger: Optional[int] = Field(default=None, init=False)
    maxBytesPerTrigger: Optional[str] = Field(default=None, init=False)

    def apply_streaming_settings(self, streaming_writer: DataStreamWriter) -> DataStreamWriter:
        if self.continuous_streaming_enabled is False:
            streaming_writer = streaming_writer.trigger(availableNow=True)
        if self.maxFilesPerTrigger is not None:
            streaming_writer = streaming_writer.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
        if self.maxBytesPerTrigger is not None:
            streaming_writer = streaming_writer.option("maxBytesPerTrigger", self.maxBytesPerTrigger)
        return streaming_writer

    class Config:
        case_sensitive = False
