import pytest
from unittest import mock
from pyspark.sql import SparkSession
from source.silver.src.silver.application.stream import execute


@mock.patch("silver.src.silver.application.stream.config")
@mock.patch("silver.src.silver.application.stream.initialize_spark")
@mock.patch("silver.src.silver.application.stream.span_record_exception")
@mock.patch("silver.src.silver.application.stream.measurements_bronze_repository")
@mock.patch("silver.src.silver.application.stream.measurements_silver_repository")
@mock.patch("silver.src.silver.application.stream.environment_helper")
@mock.patch("silver.src.silver.application.stream.path_helper")
def test__execute__should_be_success(
    mock_measurements_silver_repository,
    mock_measurements_bronze_repository,
    mock_span_record_exception,
    mock_initialize_spark,
    mock_config,
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
    mock_span_record_exception.assert_not_called()

@mock.patch("silver.src.silver.application.stream.config")
@mock.patch("silver.src.silver.application.stream.initialize_spark")
@mock.patch("silver.src.silver.application.stream.span_record_exception")
@mock.patch("silver.src.silver.application.stream.measurements_bronze_repository")
@mock.patch("silver.src.silver.application.stream.measurements_silver_repository")
@mock.patch("silver.src.silver.application.stream.environment_helper")
@mock.patch("silver.src.silver.application.stream.path_helper")
def test__execute__should_throw_exception(
    mock_span_record_exception,
    mock_initialize_spark,
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