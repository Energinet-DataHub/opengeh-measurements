from unittest.mock import Mock

from pyspark.sql import SparkSession

from src.core.gold.application.ports.gold_port import GoldPort
from src.core.gold.application.ports.silver_port import SilverPort
from src.core.gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import (
    StreamProcessorMeasurements,
)
from src.core.gold.infrastructure.config import GoldTableNames
from src.core.silver.infrastructure.config import SilverTableNames


def test__stream_processor_measurements__calls_expected(spark: SparkSession):
    # Arrange
    silver_port_mock = Mock(spec=SilverPort)
    gold_port_mock = Mock(spec=GoldPort)
    silver_target_table = SilverTableNames.silver_measurements
    gold_target_table = GoldTableNames.gold_measurements
    stream_processor = StreamProcessorMeasurements(
        silver_port_mock, silver_target_table, gold_port_mock, gold_target_table
    )

    # Act
    stream_processor.stream_measurements_silver_to_gold()

    # Assert
    silver_port_mock.read_stream.assert_called_once_with(silver_target_table, {"ignoreDeletes": "true"})
    gold_port_mock.start_write_stream.assert_called_once_with(
        silver_port_mock.read_stream.return_value,
        "measurements_silver_to_gold",
        gold_target_table,
        stream_processor.pipeline_measurements_silver_to_gold,
    )


def test__pipeline_measurements_silver_to_gold__calls_append_to_gold_measurements(spark: SparkSession):
    # Arrange
    silver_port_mock = Mock(spec=SilverPort)
    gold_port_mock = Mock(spec=GoldPort)
    silver_target_table = SilverTableNames.silver_measurements
    gold_target_table = GoldTableNames.gold_measurements
    stream_processor = StreamProcessorMeasurements(
        silver_port_mock, silver_target_table, gold_port_mock, gold_target_table
    )
    df_silver_mock = Mock()
    batch_id = 0

    # Act
    stream_processor.pipeline_measurements_silver_to_gold(df_silver_mock, batch_id)

    # Assert
    gold_port_mock.append.assert_called_once()
