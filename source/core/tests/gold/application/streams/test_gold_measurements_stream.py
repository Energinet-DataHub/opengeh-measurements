from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

import core.gold.application.streams.gold_measurements_stream as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.gold.infrastructure.config import GoldTableNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__stream_measurements_silver_to_gold__calls_expected(spark: SparkSession):
    # Arrange
    silver_repo_mock = Mock(spec=SilverMeasurementsRepository)
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    with (
        patch.object(sut, "SilverMeasurementsRepository", return_value=silver_repo_mock),
        patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock),
    ):
        silver_repo_mock.read_stream.return_value = Mock()

        # Act
        sut.stream_measurements_silver_to_gold()

        # Assert
        silver_repo_mock.read_stream.assert_called_once()
        gold_repo_mock.write_stream.assert_called_once_with(
            silver_repo_mock.read_stream.return_value,
            sut._batch_operation,
        )


def test__pipeline_measurements_silver_to_gold__calls_append_to_gold_measurements(spark: SparkSession):
    # Arrange
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    transform_mock = Mock()
    with (
        patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock),
        patch.object(sut.transformations, "transform_silver_to_gold", transform_mock),
    ):
        silver_measurements_mock = Mock()

        # Act
        sut._batch_operation(silver_measurements_mock, 0)

        # Assert
        transform_mock.assert_called_once_with(silver_measurements_mock)
        gold_repo_mock.append_if_not_exists.assert_called_once_with(transform_mock.return_value)


def test__stream_measurements_silver_to_gold__append_to_gold_measurements(
    spark: SparkSession, migrations_executed, mock_checkpoint_path
) -> None:
    # Arrange
    metering_point_id = identifier_helper.create_random_metering_point_id()
    silver_measurements = SilverMeasurementsBuilder(spark).add_row(metering_point_id=metering_point_id).build()
    table_helper.append_to_table(
        silver_measurements, SilverSettings().silver_database_name, SilverTableNames.silver_measurements
    )

    # Act
    sut.stream_measurements_silver_to_gold()

    # Arrange
    gold_measurements = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"metering_point_id = '{metering_point_id}'"
    )
    assert gold_measurements.count() == 24
