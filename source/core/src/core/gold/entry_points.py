from core.gold.application.config.spark import initialize_spark
from core.gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import (
    StreamProcessorMeasurements,
)
from core.gold.infrastructure.adapters.delta_gold_adapter import DeltaGoldAdapter
from core.gold.infrastructure.adapters.delta_silver_adapter import DeltaSilverAdapter
from core.gold.infrastructure.config import GoldTableNames
from core.silver.infrastructure.config import SilverTableNames


def stream_silver_to_gold_measurements() -> None:
    spark = initialize_spark()
    silver_adapter = DeltaSilverAdapter(spark)
    gold_adapter = DeltaGoldAdapter()

    silver_source_table = SilverTableNames.silver_measurements
    gold_target_table = GoldTableNames.gold_measurements
    measurements_stream_processor = StreamProcessorMeasurements(
        silver_adapter, silver_source_table, gold_adapter, gold_target_table
    )
    measurements_stream_processor.stream_measurements_silver_to_gold()
