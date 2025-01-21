import pytest
from unittest import mock
from pyspark.sql import SparkSession
from silver.application.stream import execute


@mock.patch("source.silver.src.silver.application.stream.measurements_silver_repository")
@mock.patch("source.silver.src.silver.application.stream.measurements_bronze_repository")
@mock.patch("source.silver.src.silver.application.stream.config")
@mock.patch("source.silver.src.silver.application.stream.initialize_spark")
def test__execute__should_be_success(
    mock_initialize_spark,
    mock_config,
    mock_measurements_bronze_repository,
    mock_measurements_silver_repository,
):
    # Arrange
    mock_spark = mock.Mock(spec=SparkSession)
    mock_initialize_spark.return_value = mock_spark

    # Act
    execute()

    # Assert
    mock_config.configure_logging.assert_called_once()
    mock_initialize_spark.assert_called_once()
    mock_measurements_bronze_repository.Repository.assert_called_once()
    mock_measurements_silver_repository.Repository.assert_called_once()

@mock.patch("source.silver.src.silver.application.stream.span_record_exception")
@mock.patch("source.silver.src.silver.application.stream.initialize_spark")
def test__execute__should_throw_exception(
    mock_initialize_spark,
    mock_span_record_exception,
):
    # Arrange
    mock_initialize_spark.side_effect = Exception("Test Exception")

    # Act
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        execute()

    # Assert
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 4
    mock_span_record_exception.assert_called_once()