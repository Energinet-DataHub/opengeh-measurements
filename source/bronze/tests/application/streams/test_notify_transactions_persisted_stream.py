from unittest.mock import Mock

import opengeh_bronze.application.streams.notify_transactions_persisted_stream as sut


def test__notify__should_call_expected(mocker: Mock) -> None:
    # Arrange
    mocked_bronze_repository = mocker.patch.object(
        sut.BronzeRepository, sut.BronzeRepository.read_submitted_transactions.__name__
    )
    mocked_kafka_stream = mocker.patch.object(sut.KafkaStream, sut.KafkaStream.__init__.__name__, return_value=None)
    mocked_kafka_stream_write = mocker.patch.object(sut.KafkaStream, sut.KafkaStream.write_stream.__name__)

    # Act
    sut.notify()

    # Assert
    mocked_bronze_repository.assert_called_once()
    mocked_kafka_stream.assert_called_once()
    mocked_kafka_stream_write.assert_called_once()
