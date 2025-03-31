import datetime

from pyspark.sql import SparkSession
from pytest_mock import MockFixture

from core.bronze.infrastructure.migration_data.silver_time_series_repository import (
    MigrationsSilverTimeSeriesRepository,
)


def test__create_chunks_of_partitions_for_data_with_a_single_partition_col__creates_balanced_partitions(
    spark: SparkSession, mocker: MockFixture
) -> None:
    # Arrange
    repo = MigrationsSilverTimeSeriesRepository(spark)

    dates = [
        datetime.date(2025, 1, 1),
        datetime.date(2025, 2, 1),
        datetime.date(2025, 3, 1),
        datetime.date(2025, 4, 1),
        datetime.date(2025, 5, 1),
        datetime.date(2025, 4, 1),
    ]

    # Create a DataFrame from the list of dates
    df = spark.createDataFrame([(date,) for date in dates], ["date"])

    mocker.patch.object(repo, repo.read_migrations_silver_time_series.__name__, return_value=df)

    # Act
    chunks = repo.create_chunks_of_partitions_for_data_with_a_single_partition_col(partition_col="date", num_chunks=3)

    # Assert
    assert len(chunks) == 3
    assert max([len(chunk) for chunk in chunks]) == 2
    assert min([len(chunk) for chunk in chunks]) == 1
    assert sum([len(chunk) for chunk in chunks]) == len(set(dates))


def test__create_chunks_of_partitions_for_data_with_a_single_partition_col__creates_no_empty_chunks(
    spark: SparkSession, mocker: MockFixture
) -> None:
    # Arrange
    repo = MigrationsSilverTimeSeriesRepository(spark)

    dates = [
        datetime.date(2025, 1, 1),
    ]

    # Create a DataFrame from the list of dates
    df = spark.createDataFrame([(date,) for date in dates], ["date"])

    mocker.patch.object(repo, repo.read_migrations_silver_time_series.__name__, return_value=df)

    # Act
    chunks = repo.create_chunks_of_partitions_for_data_with_a_single_partition_col(
        partition_col="date", num_chunks=len(dates) * 100
    )

    # Assert
    assert len(chunks) == len(dates)
    assert min([len(chunk) for chunk in chunks]) > 0
