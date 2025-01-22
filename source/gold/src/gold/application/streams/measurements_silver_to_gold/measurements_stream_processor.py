from gold.application.ports.gold_repository import GoldRepository
from gold.application.ports.silver_repository import SilverRepository
from gold.domain.streams.silver_to_gold.transformations import explode_silver_points
from gold.infrastructure.config.table_names import TableNames
from pyspark.sql.dataframe import DataFrame


class MeasurementsStreamProcessor:
    def __init__(self, silver_repository: SilverRepository, gold_repository: GoldRepository):
        self.silver_repository = silver_repository
        self.gold_repository = gold_repository

    def stream_measurements_silver_to_gold(self, silver_source_table: str, gold_target_table: str, query_name: str) -> None:
        df_silver_stream = self.silver_repository.read_stream(silver_source_table, {"ignoreDeletes": "true"})
        self.gold_repository.start_write_stream(df_silver_stream, query_name, gold_target_table, self.silver_to_measurements_gold_pipeline)

    def silver_to_measurements_gold_pipeline(self, df_silver: DataFrame, batch_id: int) -> None:
        exploded_records = explode_silver_points(df_silver)
        self.gold_repository.append(exploded_records, TableNames.gold_measurements_table)
