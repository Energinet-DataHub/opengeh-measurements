from unittest.mock import Mock

from pytest_mock import MockFixture

import core.gold.application.streams.gold_measurements_stream as sut
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository


def test__stream_measurements_silver_to_gold__calls_expected(mocker: MockFixture):
    # Arrange
    silver_repo_mock = mocker.patch.object(sut, sut.SilverMeasurementsRepository.__name__)
    gold_repo_mock = mocker.patch.object(sut, sut.GoldMeasurementsStream.__name__)

    # Act
    sut.stream_measurements_silver_to_gold()

    # Assert
    silver_repo_mock.assert_called_once()
    gold_repo_mock.assert_called_once()


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
