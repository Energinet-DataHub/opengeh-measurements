from unittest.mock import Mock

from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core.gold.application.streams.calculated_measurements_stream as sut
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.calculated_measurements_repository import CalculatedMeasurementsRepository
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.gold.infrastructure.streams.gold_measurements_stream import GoldMeasurementsStream


def test__stream_measurements_calculated_to_gold__calls_expected(spark: SparkSession, mocker: MockerFixture):
    # Arrange
    calculated_repo_mock = Mock(spec=CalculatedMeasurementsRepository)
    gold_stream_mock = Mock(spec=GoldMeasurementsStream)
    mocker.patch.object(sut, "CalculatedMeasurementsRepository", return_value=calculated_repo_mock)
    mocker.patch.object(sut, "GoldMeasurementsStream", return_value=gold_stream_mock)

    # Act
    sut.stream_measurements_calculated_to_gold()

    # Assert
    calculated_repo_mock.read_stream.assert_called_once()
    gold_stream_mock.write_stream.assert_called_once_with(
        "measurements_calculated",
        "measurements_calculated_to_gold",
        calculated_repo_mock.read_stream.return_value,
        sut._batch_operation,
    )


def test__batch_operation__calls_append_to_gold_measurements(mocker: MockerFixture):
    # Arrange
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    transform_mock = Mock()
    mocker.patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock)
    mocker.patch.object(sut.transformations, "transform_calculated_to_gold", transform_mock)
    calculated_measurements_mock = Mock()

    # Act
    sut._batch_operation(calculated_measurements_mock, 0)

    # Assert
    transform_mock.assert_called_once_with(calculated_measurements_mock)
    gold_repo_mock.append_if_not_exists.assert_called_once_with(
        transform_mock.return_value, query_name=QueryNames.CALCULATED_TO_GOLD
    )
