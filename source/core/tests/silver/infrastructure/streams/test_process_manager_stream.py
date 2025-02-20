from unittest import mock

import tests.helpers.environment_variables_helpers as environment_variables_helpers
from core.silver.infrastructure.streams.process_manager_stream import ProcessManagerStream


@mock.patch("core.silver.infrastructure.streams.process_manager_stream.get_checkpoint_path")
def test__write_stream__should_set_environment_variables(mock_get_checkpoint_path):
    # Arrange
    mock_get_checkpoint_path.return_value = "/tmp/checkpoints/start_write_stream_test"
    environment_variables_helpers.set_kafka_authentication_settings()
    environment_variables_helpers.set_storage_account_settings()
    process_manager_stream = ProcessManagerStream()
    dataframe_mock = mock.Mock()

    # Act & Assert
    process_manager_stream.write_stream(dataframe_mock)
