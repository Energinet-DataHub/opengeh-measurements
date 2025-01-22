from gold.application.ports.gold_repository import GoldRepository
from gold.application.ports.silver_repository import SilverRepository
from gold.domain.streams.silver_to_gold.transformations import explode_silver_points
from pyspark.sql.dataframe import DataFrame


class MeasurementsStreamProcessor:
    def __init__(self, silver_repository: SilverRepository, silver_target_table: str, gold_repository: GoldRepository, gold_target_table: str):
        self.silver_repository = silver_repository
        self.silver_target_table = silver_target_table
        self.gold_repository = gold_repository
        self.gold_target_table = gold_target_table
        self.query_name = "measurements_silver_to_gold"

    def stream_measurements_silver_to_gold(self) -> None:
        df_silver_stream = self.silver_repository.read_stream(self.silver_target_table, {"ignoreDeletes": "true"})
        self.gold_repository.start_write_stream(df_silver_stream, self.query_name, self.gold_target_table, self.silver_to_measurements_gold_pipeline)

    def silver_to_measurements_gold_pipeline(self, df_silver: DataFrame, batch_id: int) -> None:
        exploded_records = explode_silver_points(df_silver)
        self.gold_repository.append(exploded_records, self.gold_target_table)
