from datetime import datetime

from pyspark.sql import SparkSession

import core.utility.delta_table_helper as delta_table_helper


def test__get_target_filter_for_datetime_clustering_key__should_return_correctly_formed_filter(
    spark: SparkSession,
) -> None:
    # Arrange
    dates = [
        datetime(2025, 4, 1),
        datetime(2025, 4, 2),
        datetime(2025, 4, 4),
        datetime(2025, 4, 5),
        datetime(2025, 4, 6),
        datetime(2025, 4, 8),
    ]

    alias = "target"
    clustering_col = "date"
    df = spark.createDataFrame([(date,) for date in dates], [clustering_col]).alias(alias)

    # Act
    filter_statement = delta_table_helper.get_target_filter_for_datetime_clustering_key(df, clustering_col, alias)

    # Assert that our filter keeps all input rows, and does not raise an AnalysisException on a table/dataframe with the given alias.
    assert df.filter(filter_statement).count() == df.count()
