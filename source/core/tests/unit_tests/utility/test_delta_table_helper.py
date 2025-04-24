from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core.utility.delta_table_helper as sut


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
