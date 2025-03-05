from unittest.mock import Mock

from pyspark.sql import SparkSession

import core.gold.application.streams.gold_measurements_stream as sut
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.silver.infrastructure.streams.silver_repository import SilverRepository


def test__stream_measurements_silver_to_gold__calls_expected(spark: SparkSession):
    # Arrange
    silver_repo_mock = Mock(spec=SilverRepository)
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    sut.SilverRepository = Mock(return_value=silver_repo_mock)
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
    df_silver_mock = Mock()
    batch_id = 0

    # Act
    sut.pipeline_measurements_silver_to_gold(df_silver_mock, batch_id)

    # Assert
    gold_repo_mock.append_if_not_exists.assert_called_once_with(df_silver_mock)
