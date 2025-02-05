import src.core.migrations.migrations_runner as migrations_runner
from src.core.gold.application.config.spark import initialize_spark
from src.core.gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import (
    StreamProcessorMeasurements,
)
from src.core.gold.infrastructure.adapters.delta_gold_adapter import DeltaGoldAdapter
from src.core.gold.infrastructure.adapters.delta_silver_adapter import DeltaSilverAdapter
from src.core.gold.infrastructure.config.table_names import TableNames


# Todo, move to core package entry_points.py
def migrate_gold() -> None:
    migrations_runner.migrate()


def stream_silver_to_gold_measurements() -> None:
    spark = initialize_spark()
    silver_adapter = DeltaSilverAdapter(spark)
    gold_adapter = DeltaGoldAdapter()

    silver_source_table = TableNames.silver_measurements
    gold_target_table = TableNames.gold_measurements
    measurements_stream_processor = StreamProcessorMeasurements(
        silver_adapter, silver_source_table, gold_adapter, gold_target_table
    )
    measurements_stream_processor.stream_measurements_silver_to_gold()
