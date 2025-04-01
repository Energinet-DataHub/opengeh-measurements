from abc import ABC
from typing import Callable

from pyspark.sql import DataFrame

from core.settings.streaming_settings import StreamingSettings


class Stream(ABC):
    def __init__(self, name: str):
        self.name = name

    def write_stream(
        self,
        dataframe: DataFrame,
        checkpoint_location: str,
        query_name: str,
        batch_operation: Callable[["DataFrame", int], None],
        outputMode: str = "append",
        format: str = "delta",
    ) -> bool | None:
        write_stream = (
            dataframe.writeStream.outputMode(outputMode)
            .format(format)
            .queryName(query_name)
            .option("checkpointLocation", checkpoint_location)
        )

        stream_settings = StreamingSettings()
        write_stream = stream_settings.apply_streaming_settings(write_stream)

        return write_stream.foreachBatch(batch_operation).start().awaitTermination()
