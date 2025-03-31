from unittest import mock

from pytest_mock import MockFixture

import core.bronze.infrastructure.streams.kafka_stream as sut
import tests.helpers.environment_variables_helpers as environment_variables_helpers
from core.bronze.infrastructure.streams.kafka_stream import KafkaStream


def test__submit_transactions__should_set_environment_variables(mocker: MockFixture):
    # Arrange
    mock_get_checkpoint_path = mocker.patch.object(
        sut.shared_helpers,
        sut.shared_helpers.get_checkpoint_path.__name__,
    )
    mock_get_checkpoint_path.return_value = "/tmp/checkpoints/start_write_stream_test"
    environment_variables_helpers.set_kafka_authentication_settings()
    environment_variables_helpers.set_storage_account_settings()
    environment_variables_helpers.set_submitted_transactions_stream_settings()
    kafka_stream = KafkaStream()
    spark_mock = mock.Mock()

    # Act & Assert
    kafka_stream.submit_transactions(spark_mock)
