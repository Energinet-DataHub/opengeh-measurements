from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

import core.gold.application.streams.calculated_measurements_stream as sut
from core.gold.infrastructure.repositories.calculated_measurements_repository import CalculatedMeasurementsRepository
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository


def test__stream_measurements_calculated_to_gold__calls_expected(spark: SparkSession):
    # Arrange
    calculated_repo_mock = Mock(spec=CalculatedMeasurementsRepository)
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    with (
        patch.object(sut, "CalculatedMeasurementsRepository", return_value=calculated_repo_mock),
        patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock),
    ):
        calculated_repo_mock.read_stream.return_value = Mock()

        # Act
        sut.stream_measurements_calculated_to_gold()

        # Assert
        calculated_repo_mock.read_stream.assert_called_once()
        gold_repo_mock.write_stream.assert_called_once_with(
            "measurements_calculated_to_gold",
            calculated_repo_mock.read_stream.return_value,
            sut._batch_operation,
        )


def test__pipeline_measurements_calculated_to_gold__calls_append_to_gold_measurements(spark: SparkSession):
    # Arrange
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    transform_mock = Mock()
    with (
        patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock),
        patch.object(sut.transformations, "transform_calculated_to_gold", transform_mock),
    ):
        calculated_measurements_mock = Mock()

        # Act
        sut._batch_operation(calculated_measurements_mock, 0)

        # Assert
        transform_mock.assert_called_once_with(calculated_measurements_mock)
        gold_repo_mock.append_if_not_exists.assert_called_once_with(transform_mock.return_value)
