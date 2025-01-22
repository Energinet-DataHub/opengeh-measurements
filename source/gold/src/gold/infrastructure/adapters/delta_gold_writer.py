from typing import Callable

from pyspark.sql import DataFrame

from gold.application.ports.gold_writer import GoldWriter
from gold.infrastructure.config.delta_gold_config import DeltaGoldWriterConfig


class DeltaGoldWriter(GoldWriter):
    def __init__(self, config: DeltaGoldWriterConfig):
        config.validate()
        self.config = config

    def start(self, records: DataFrame, batch_operation: Callable[["DataFrame", int], None]) -> None:
        (records.writeStream
         .format("delta")
         .queryName(self.config.query_name)
         .option("checkpointLocation", self.config.checkpoint_location)
         .foreachBatch(batch_operation)
         .trigger(availableNow=True)
         .start()
         .awaitTermination())

    def write(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").saveAsTable(self.config.gold_path)
