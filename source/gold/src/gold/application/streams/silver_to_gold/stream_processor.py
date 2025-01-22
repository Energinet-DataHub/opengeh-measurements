from gold.application.ports.gold_writer import GoldWriter
from gold.application.ports.silver_reader import SilverReader
from gold.domain.streams.silver_to_gold.transformations import explode_silver_points


class StreamProcessor:
    def __init__(self, silver_reader: SilverReader, gold_writer: GoldWriter):
        self.silver_reader = silver_reader
        self.gold_writer = gold_writer

    def execute_silver_to_gold_stream(self) -> None:
        records = self.silver_reader.read()
        self.gold_writer.start(records, self.silver_to_gold_pipeline)

    def silver_to_gold_pipeline(self, df_silver, batch_id: int) -> None:
        exploded_records = explode_silver_points(df_silver)
        self.gold_writer.write(exploded_records)
