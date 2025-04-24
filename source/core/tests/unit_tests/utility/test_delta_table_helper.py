from datetime import datetime

from pyspark.sql import SparkSession

import core.utility.delta_table_helper as delta_table_helper


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
