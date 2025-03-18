from unittest.mock import Mock

import core.gold.entry_points as sut


def test__stream_silver_to_gold_measurements__should_call_expected() -> None:
    # Arrange
    stream_measurements_silver_to_gold_mock = Mock()
    sut.gold_measurements_stream.stream_measurements_silver_to_gold = stream_measurements_silver_to_gold_mock

    # Act
    sut.stream_silver_to_gold_measurements()

    # Assert
    stream_measurements_silver_to_gold_mock.assert_called_once()
