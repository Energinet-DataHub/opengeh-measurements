from gold.application.ports.gold_port import GoldPort
from gold.application.ports.silver_port import SilverPort
from gold.domain.streams.silver_to_gold.transformations import explode_silver_points
from pyspark.sql.dataframe import DataFrame


class StreamProcessorMeasurements:
    def __init__(self, silver_port: SilverPort, silver_target_table: str, gold_port: GoldPort, gold_target_table: str):
        self.silver_port = silver_port
        self.silver_target_table = silver_target_table
        self.gold_port = gold_port
        self.gold_target_table = gold_target_table
        self.query_name = "measurements_silver_to_gold"

    def stream_measurements_silver_to_gold(self) -> None:
        df_silver_stream = self.silver_port.read_stream(self.silver_target_table, {"ignoreDeletes": "true"})
        self.gold_port.start_write_stream(df_silver_stream, self.query_name, self.gold_target_table, self.pipeline_measurements_silver_to_gold)

    def pipeline_measurements_silver_to_gold(self, df_silver: DataFrame, batch_id: int) -> None:
        exploded_records = explode_silver_points(df_silver)
        self.gold_port.append(exploded_records, self.gold_target_table)
