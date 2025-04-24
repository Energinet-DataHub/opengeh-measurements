from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from pytest_mock import MockerFixture

import core.utility.delta_table_helper as delta_table_helper
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
    mocked_delta_table.forName().alias().merge.assert_called_once()
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

    expected_condition = "current.id <=> update.id AND ((CAST(current.clustering_column AS DATE) >= '2023-01-01' AND CAST(current.clustering_column AS DATE) <= '2023-01-02'))"

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
    mocked_delta_table.forName().alias().merge.assert_called_once()
    assert mocked_delta_table.forName().alias().merge.call_args[1]["condition"] == expected_condition
    mocked_delta_table.forName().alias().merge().whenNotMatchedInsertAll.assert_called_once()


def test__append_if_not_exists__when_multiple_clustering_columns_to_filter__calls_expected(
    spark: SparkSession, mocker: MockerFixture
) -> None:
    # Arrange
    mocked_delta_table = mocker.patch(f"{sut.__name__}.DeltaTable")
    table_name = "test_table"

    data = [
        (
            1,
            datetime_helper.get_datetime(2023, 1, 1),
            datetime_helper.get_datetime(2023, 1, 2),
        ),
        (
            2,
            datetime_helper.get_datetime(2023, 1, 2),
            datetime_helper.get_datetime(2023, 1, 4),
        ),
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("clustering_column_1", TimestampType(), True),
            StructField("clustering_column_2", TimestampType(), True),
        ]
    )

    dataframe = spark.createDataFrame(data, schema)
    merge_columns = ["id"]
    clustering_columns_to_filter_specifically = ["clustering_column_1", "clustering_column_2"]

    expected_condition = "current.id <=> update.id AND ((CAST(current.clustering_column_1 AS DATE) >= '2023-01-01' AND CAST(current.clustering_column_1 AS DATE) <= '2023-01-02')) AND (CAST(current.clustering_column_2 AS DATE) == '2023-01-02' OR CAST(current.clustering_column_2 AS DATE) == '2023-01-04')"

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
    mocked_delta_table.forName().alias().merge.assert_called_once()
    assert mocked_delta_table.forName().alias().merge.call_args[1]["condition"] == expected_condition
    mocked_delta_table.forName().alias().merge().whenNotMatchedInsertAll.assert_called_once()


def test__append_if_not_exists__when_target_filters__calls_expected(spark: SparkSession, mocker: MockerFixture) -> None:
    # Arrange
    mocked_delta_table = mocker.patch(f"{sut.__name__}.DeltaTable")
    table_name = "test_table"

    data = [(1, "submitted"), (2, "migration")]

    schema = StructType([StructField("id", IntegerType(), True), StructField("clustering_column", StringType(), True)])

    dataframe = spark.createDataFrame(data, schema)
    merge_columns = ["id"]
    target_filters = {"clustering_column": ["submitted", "migration"]}

    expected_condition = "current.id <=> update.id AND current.clustering_column in ('submitted','migration')"

    # Act
    sut.append_if_not_exists(
        spark=spark,
        dataframe=dataframe,
        table=table_name,
        merge_columns=merge_columns,
        target_filters=target_filters,
    )

    # Arrange
    mocked_delta_table.forName.assert_called_once_with(spark, table_name)
    mocked_delta_table.forName().alias().merge.assert_called_once()
    assert mocked_delta_table.forName().alias().merge.call_args[1]["condition"] == expected_condition
    mocked_delta_table.forName().alias().merge().whenNotMatchedInsertAll.assert_called_once()


def test__get_target_filter_for_datetime_clustering_key__should_return_correctly_formed_filter(
    spark: SparkSession,
) -> None:
    # Arrange
    dates_and_occurences = [
        (datetime(2025, 4, 1), 1),  # Start of an interval.
        (datetime(2025, 4, 2), 1),  # End of an interval.
        (datetime(2025, 4, 4), 1),  # Start of an interval.
        (datetime(2025, 4, 5), 0),  # Covered by an interval
        (datetime(2025, 4, 6), 1),  # End of an interval.
        (datetime(2025, 4, 8), 1),  # Start and end of an interval.
    ]

    alias = "target"
    clustering_col = "date"
    df = spark.createDataFrame([(date[0],) for date in dates_and_occurences], [clustering_col]).alias(alias)

    # Act
    filter_statement = delta_table_helper.get_target_filter_for_datetime_clustering_key(df, clustering_col, alias)

    # Assert that our filter keeps all input rows, and does not raise an AnalysisException on a table/dataframe with the given alias.
    assert df.filter(filter_statement).count() == df.count()

    for date, expected_occurences in dates_and_occurences:
        assert filter_statement.count(date.strftime("%Y-%m-%d")) == expected_occurences


def test__get_target_filter_for_datetime_clustering_key__should_return_simple_true_filter_on_empty_input(
    spark: SparkSession,
) -> None:
    # Arrange
    dates = [
        datetime(2025, 4, 1),
        datetime(2025, 4, 2),
        datetime(2025, 4, 4),
    ]

    alias = "target"
    clustering_col = "date"
    df = spark.createDataFrame([(date,) for date in dates], [clustering_col]).alias(alias)

    # Act
    filter_statement_from_empty_input = delta_table_helper.get_target_filter_for_datetime_clustering_key(
        df.limit(0), clustering_col, alias
    )

    # Assert that our filter keeps all input rows, and does not raise an AnalysisException on a table/dataframe with the given alias.
    assert df.filter(filter_statement_from_empty_input).count() == df.count()
