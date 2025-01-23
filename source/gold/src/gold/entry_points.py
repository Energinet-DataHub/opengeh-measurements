import gold.migrations.migrations_runner as migrations_runner
from gold.application.config.spark import initialize_spark
from gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import (
    StreamProcessorMeasurements,
)
from gold.infrastructure.adapters.delta_gold_adapter import DeltaGoldAdapter
from gold.infrastructure.adapters.delta_silver_adapter import DeltaSilverAdapter
from gold.infrastructure.config.table_names import TableNames


def migrate_gold() -> None:
    migrations_runner.migrate()


def stream_silver_to_gold_measurements() -> None:
    spark = initialize_spark()
    silver_port = DeltaSilverAdapter(spark)
    gold_port = DeltaGoldAdapter()

    silver_source_table = TableNames.silver_measurements_table
    gold_target_table = TableNames.gold_measurements_table
    measurements_stream_processor = StreamProcessorMeasurements(silver_port, silver_source_table, gold_port, gold_target_table)
    measurements_stream_processor.stream_measurements_silver_to_gold()
