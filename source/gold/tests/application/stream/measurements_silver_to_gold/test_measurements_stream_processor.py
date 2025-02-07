from unittest.mock import Mock

from pyspark.sql import SparkSession

from opengeh_gold.application.ports.gold_port import GoldPort
from opengeh_gold.application.ports.silver_port import SilverPort
from opengeh_gold.application.streams.measurements_silver_to_gold.measurements_stream_processor import (
    StreamProcessorMeasurements,
)
from opengeh_gold.infrastructure.config.table_names import TableNames


def test__stream_processor_measurements__calls_expected(spark: SparkSession):
    # Arrange
    silver_port_mock = Mock(spec=SilverPort)
    gold_port_mock = Mock(spec=GoldPort)
    silver_target_table = TableNames.silver_measurements
    gold_target_table = TableNames.gold_measurements
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
    silver_target_table = TableNames.silver_measurements
    gold_target_table = TableNames.gold_measurements
    stream_processor = StreamProcessorMeasurements(
        silver_port_mock, silver_target_table, gold_port_mock, gold_target_table
    )
    df_silver_mock = Mock()
    batch_id = 0

    # Act
    stream_processor.pipeline_measurements_silver_to_gold(df_silver_mock, batch_id)

    # Assert
    gold_port_mock.append.assert_called_once()
