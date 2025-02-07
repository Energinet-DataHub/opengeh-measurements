from unittest import mock

import bronze.helpers.environment_variables_helpers as environment_variables_helpers

from core.bronze.infrastructure.streams.kafka_stream import KafkaStream


@mock.patch("core.bronze.infrastructure.streams.kafka_stream.get_checkpoint_path")
def test__submit_transactions__should_set_environment_variables(mock_get_checkpoint_path):
    # Arrange
    mock_get_checkpoint_path.return_value = "/tmp/checkpoints/start_write_stream_test"
    environment_variables_helpers.set_kafka_authentication_settings()
    environment_variables_helpers.set_storage_account_settings()
    environment_variables_helpers.set_submitted_transactions_stream_settings()
    kafka_stream = KafkaStream()
    spark_mock = mock.Mock()

    # Act & Assert
    kafka_stream.submit_transactions(spark_mock)


@mock.patch("core.bronze.infrastructure.streams.kafka_stream.get_checkpoint_path")
def test__write_stream__should_set_environment_variables(mock_get_checkpoint_path):
    # Arrange
    mock_get_checkpoint_path.return_value = "/tmp/checkpoints/start_write_stream_test"
    environment_variables_helpers.set_kafka_authentication_settings()
    environment_variables_helpers.set_storage_account_settings()
    kafka_stream = KafkaStream()
    dataframe_mock = mock.Mock()

    # Act & Assert
    kafka_stream.write_stream(dataframe_mock)
