from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType
from pytest_mock import MockerFixture

import core.utility.delta_table_helper as sut
import tests.helpers.datetime_helper as datetime_helper


def test__append_if_not_exists__calls_expected(spark: SparkSession, mocker: MockerFixture) -> None:
    # Arrange
    mocked_delta_table = mocker.patch(f"{sut.__name__}.DeltaTable")
    table_name = "test_table"

    mocked_dataframe = mocker.Mock()
    merge_columns = ["id"]

    # Act
    sut.append_if_not_exists(
        spark=spark,
        dataframe=mocked_dataframe,
        table=table_name,
        merge_columns=merge_columns,
    )

    # Arrange
    mocked_dataframe.dropDuplicates.assert_called_once_with(subset=merge_columns)

    mocked_delta_table.forName.assert_called_once_with(spark, table_name)
    mocked_delta_table.forName().alias().merge.assert_called_once_with()
    mocked_delta_table.forName().alias().merge().whenNotMatchedInsertAll.assert_called_once()


def test__append_if_not_exists__when_clustering_columns_to_filter_specifically__calls_expected(
    spark: SparkSession, mocker: MockerFixture
) -> None:
    # Arrange
    mocked_delta_table = mocker.patch(f"{sut.__name__}.DeltaTable")
    table_name = "test_table"

    data = [(1, datetime_helper.get_datetime(2023, 1, 1)), (2, datetime_helper.get_datetime(2023, 1, 2))]

    schema = StructType(
        [StructField("id", IntegerType(), True), StructField("clustering_column", TimestampType(), True)]
    )

    dataframe = spark.createDataFrame(data, schema)
    merge_columns = ["id"]
    clustering_columns_to_filter_specifically = ["clustering_column"]

    expected_condition = (
        "current.id <=> update.id AND CAST(current.clustering_column AS DATE) in ('2023-01-01','2023-01-02')"
    )

    # Act
    sut.append_if_not_exists(
        spark=spark,
        dataframe=dataframe,
        table=table_name,
        merge_columns=merge_columns,
        clustering_columns_to_filter_specifically=clustering_columns_to_filter_specifically,
    )

    # Arrange
    mocked_delta_table.forName.assert_called_once_with(spark, table_name)
    mocked_delta_table.forName().alias().merge.assert_called_once_with(Any, expected_condition)
    mocked_delta_table.forName().alias().merge().whenNotMatchedInsertAll.assert_called_once()
