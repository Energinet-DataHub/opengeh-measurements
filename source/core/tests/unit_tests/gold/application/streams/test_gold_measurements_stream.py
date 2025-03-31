from unittest.mock import Mock

from pytest_mock import MockFixture

import core.gold.application.streams.gold_measurements_stream as sut
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository


def test__stream_measurements_silver_to_gold__calls_expected(mocker: MockFixture):
    # Arrange
    mock_gold_measurements_repository = Mock()
    mock_silver_measurements_repository = Mock()

    # Act
    sut.stream_measurements_silver_to_gold(
        gold_measurements_repository=mock_gold_measurements_repository,
        silver_measurements_repository=mock_silver_measurements_repository,
    )

    # Assert
    mock_silver_measurements_repository.read_stream.assert_called_once()
    mock_gold_measurements_repository.write_stream.assert_called_once_with(
        mock_silver_measurements_repository.read_stream.return_value,
        sut._batch_operation,
    )


def test__pipeline_measurements_silver_to_gold__calls_append_to_gold_measurements(mocker: MockFixture):
    # Arrange
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    transform_mock = Mock()
    mocker.patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock)
    mocker.patch.object(sut.transformations, "transform_silver_to_gold", transform_mock)
    silver_measurements_mock = Mock()

    # Act
    sut._batch_operation(silver_measurements_mock, 0)

    # Assert
    transform_mock.assert_called_once_with(silver_measurements_mock)
    gold_repo_mock.append_if_not_exists.assert_called_once_with(transform_mock.return_value)
