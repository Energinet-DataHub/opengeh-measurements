from unittest.mock import Mock

from pyspark.sql import SparkSession

import core.gold.application.streams.gold_measurements_stream as sut
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository


def test__stream_measurements_silver_to_gold__calls_expected(spark: SparkSession):
    # Arrange
    silver_repo_mock = Mock(spec=SilverMeasurementsRepository)
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    sut.GoldMeasurementsRepository = Mock(return_value=gold_repo_mock)
    silver_repo_mock.read.return_value = Mock()

    # Act
    sut.stream_measurements_silver_to_gold()

    # Assert
    silver_repo_mock.read.assert_called_once()
    gold_repo_mock.start_write_stream.assert_called_once_with(
        silver_repo_mock.read.return_value,
        sut.pipeline_measurements_silver_to_gold,
    )


def test__pipeline_measurements_silver_to_gold__calls_append_to_gold_measurements(spark: SparkSession):
    # Arrange
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    sut.GoldMeasurementsRepository = Mock(return_value=gold_repo_mock)
    transform_mock = Mock()
    sut.transformations.transform_silver_to_gold = transform_mock
    silver_measurements_mock = Mock()

    # Act
    sut.pipeline_measurements_silver_to_gold(silver_measurements_mock, 0)

    # Assert
    transform_mock.assert_called_once_with(silver_measurements_mock)
    gold_repo_mock.append_if_not_exists.assert_called_once_with(transform_mock.return_value)
