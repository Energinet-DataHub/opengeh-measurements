from typing import Callable

from pyspark.sql import DataFrame


def write_stream(
    df_source_stream: DataFrame,
    query_name: str,
    checkpoint_path: str,
    batch_operation: Callable[["DataFrame", int], None],
    options: dict[str, str] = {},
) -> None:
    options = {"checkpointLocation": checkpoint_path}

    df_source_stream.writeStream.format("delta").queryName(query_name).options(**options).foreachBatch(
        batch_operation
    ).start().awaitTermination()
