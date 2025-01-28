from unittest.mock import Mock, patch

from opengeh_gold.infrastructure.config.table_names import TableNames
from src.opengeh_gold.entry_points import migrate_gold, stream_silver_to_gold_measurements


@patch("src.opengeh_gold.entry_points.initialize_spark")
@patch("src.opengeh_gold.entry_points.DeltaSilverAdapter")
@patch("src.opengeh_gold.entry_points.DeltaGoldAdapter")
@patch("src.opengeh_gold.entry_points.StreamProcessorMeasurements")
def test__stream_silver_to_gold_measurements__calls_processor(
    mock_stream_processor, mock_gold_adapter, mock_silver_adapter, mock_initialize_spark
):
    # Arrange
    mock_spark = Mock()
    mock_initialize_spark.return_value = mock_spark
    mock_silver_adapter_instance = Mock()
    mock_silver_adapter.return_value = mock_silver_adapter_instance
    mock_gold_adapter_instance = Mock()
    mock_gold_adapter.return_value = mock_gold_adapter_instance
    mock_stream_processor_instance = Mock()
    mock_stream_processor.return_value = mock_stream_processor_instance

    # Act
    stream_silver_to_gold_measurements()

    # Assert
    mock_initialize_spark.assert_called_once()
    mock_silver_adapter.assert_called_once_with(mock_spark)
    mock_gold_adapter.assert_called_once()
    mock_stream_processor.assert_called_once_with(
        mock_silver_adapter_instance,
        TableNames.silver_measurements_table,
        mock_gold_adapter_instance,
        TableNames.gold_measurements_table,
    )
    mock_stream_processor_instance.stream_measurements_silver_to_gold.assert_called_once()


@patch("src.opengeh_gold.entry_points.migrations_runner.migrate")
def test__migrate_gold__calls_migrate(mock_migrate):
    # Act
    migrate_gold()

    # Assert
    mock_migrate.assert_called_once()
