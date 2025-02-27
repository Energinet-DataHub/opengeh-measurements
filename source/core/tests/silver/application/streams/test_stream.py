from unittest import mock

import pytest
from pyspark.sql import DataFrame, SparkSession

from core.settings.silver_settings import SilverSettings
from core.silver.application.streams.calculated_stream import (
    _batch_operations,
    _execute,
    execute,
)
from core.silver.infrastructure.config import SilverTableNames


@mock.patch("core.silver.application.streams.calculated_stream._execute")
@mock.patch("core.silver.application.streams.calculated_stream.config")
@mock.patch("core.silver.application.streams.calculated_stream.initialize_spark")
def test__calculated_stream__should_be_success(
    mock_initialize_spark,
    mock_config,
    mock__calculated_stream,
):
    # Arrange
    mock_spark = mock.Mock(spec=SparkSession)
    mock_initialize_spark.return_value = mock_spark

    # Act
    execute()

    # Assert
    mock_config.configure_logging.assert_called_once()
    mock_initialize_spark.assert_called_once()
    mock__calculated_stream.assert_called_once()


@mock.patch("core.silver.application.streams.calculated_stream.span_record_exception")
@mock.patch("core.silver.application.streams.calculated_stream.initialize_spark")
def test__calculated_stream__should_throw_exception(
    mock_initialize_spark,
    mock_span_record_exception,
):
    # Arrange
    mock_initialize_spark.side_effect = Exception("Test Exception")

    # Act
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        execute()

    # Assert
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 4
    mock_span_record_exception.assert_called_once()


@mock.patch("core.silver.application.streams.calculated_stream.get_checkpoint_path", return_value="checkpoint")
@mock.patch("core.silver.application.streams.calculated_stream.BronzeRepository")
@mock.patch("core.silver.application.streams.calculated_stream.writer")
def test__calculated_stream_should_read_and_write(mock_writer, mock_BronzeRepository, mock_get_checkpoint_path):
    # Arrange
    mock_spark = mock.Mock(spec=SparkSession)
    mock_bronze_repository = mock_BronzeRepository.return_value
    mock_bronze_repository.read_calculated_measurements.return_value = "mock_bronze_stream"

    # Act
    _execute(mock_spark)

    # Assert
    mock_get_checkpoint_path.assert_called_once()
    mock_BronzeRepository.assert_called_once_with(mock_spark)
    mock_bronze_repository.read_calculated_measurements.assert_called_once()
    mock_writer.write_stream.assert_called_once_with(
        "mock_bronze_stream", "bronze_calculated_measurements_to_silver_measurements", "checkpoint", _batch_operations
    )


@mock.patch("core.silver.application.streams.calculated_stream.transform_calculated_measurements")
def test__batch_operations(mock_transform_calculated_measurements):
    # Arrange
    silver_settings = SilverSettings()
    mock_df = mock.Mock(spec=DataFrame)
    mock_transformed_df = mock.Mock(spec=DataFrame)
    mock_transform_calculated_measurements.return_value = mock_transformed_df
    expected_target_table_name = f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}"

    # Act
    _batch_operations(mock_df, 1)

    # Assert
    mock_transform_calculated_measurements.assert_called_once_with(mock_df)
    mock_transformed_df.write.format.assert_called_once_with("delta")
    mock_transformed_df.write.format().mode.assert_called_once_with("append")
    mock_transformed_df.write.format().mode().saveAsTable.assert_called_once_with(expected_target_table_name)
