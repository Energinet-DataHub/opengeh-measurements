import os
from unittest import mock

import tests.helpers.environment_variables_helpers as environment_variables_helpers
from core.silver.infrastructure.streams.process_manager_stream import ProcessManagerStream


def test__write_stream__calls_expected_methods(mock_checkpoint_path):
    # Arrange
    environment_variables_helpers.set_kafka_authentication_settings()
    environment_variables_helpers.set_storage_account_settings()
    process_manager_stream = ProcessManagerStream()
    dataframe_mock = mock.Mock()

    # Act
    process_manager_stream.write_stream(dataframe_mock)

    # Assert
    dataframe_mock.writeStream.format.assert_called_once_with("kafka")
    dataframe_mock.writeStream.format().options.assert_called_once_with(**process_manager_stream.kafka_options)
    dataframe_mock.writeStream.format().options().option.assert_any_call("topic", mock.ANY)
    dataframe_mock.writeStream.format().options().option().option.assert_called_once()
    dataframe_mock.writeStream.format().options().option().option().trigger.assert_called_once()
    dataframe_mock.writeStream.format().options().option().option().trigger().start.assert_called_once()


def test__write_stream__when_continous_streaming_is_disabled__should_not_call_trigger(mock_checkpoint_path):
    # Arrange
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
    environment_variables_helpers.set_kafka_authentication_settings()
    environment_variables_helpers.set_storage_account_settings()
    process_manager_stream = ProcessManagerStream()
    dataframe_mock = mock.Mock()

    # Act
    process_manager_stream.write_stream(dataframe_mock)

    # Assert
    dataframe_mock.writeStream.format().options().option().trigger.assert_not_called()
