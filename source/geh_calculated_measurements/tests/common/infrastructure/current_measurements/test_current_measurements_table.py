from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from geh_calculated_measurements.common.infrastructure.current_measurements.current_meaurements_table import (
    CurrentMeasurementsTable,
)
from tests.conftest import SPARK_CATALOG_NAME
from tests.external_data_products import ExternalDataProducts


@pytest.fixture(scope="module")
def current_measurements_table(spark: SparkSession) -> CurrentMeasurementsTable:
    return CurrentMeasurementsTable(SPARK_CATALOG_NAME)


@pytest.fixture(scope="module")
def valid_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                "123456789012345678",
                datetime(2023, 1, 1, 0, 0, 0, 0, timezone.utc),
                Decimal("1.123"),
                "measured",
                "consumption",
            )
        ],
        ExternalDataProducts.CURRENT_MEASUREMENTS.schema,
    )


def test__current_measurements_read__has_correct_schema_and_records(
    current_measurements_table: CurrentMeasurementsTable,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    def mock_read_table() -> DataFrame:
        return valid_df

    monkeypatch.setattr(current_measurements_table, "_read", mock_read_table)

    # Act
    actual = current_measurements_table.read()

    # Assert
    assert actual.collect() == valid_df.collect()
    assert actual.schema == valid_df.schema
    assert actual.schema == ExternalDataProducts.CURRENT_MEASUREMENTS.schema


def test__when_invalid_contract__raises_with_useful_message(
    current_measurements_table: CurrentMeasurementsTable,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    invalid_df = valid_df.drop(F.col("quantity"))

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return invalid_df

    monkeypatch.setattr(current_measurements_table, "_read", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"The data source does not comply with the contract.*",
    ):
        # Act
        current_measurements_table.read()


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    current_measurements_table: CurrentMeasurementsTable,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the table can handle columns being added as it is defined to _not_ be a breaking change.
    The table should return the data without the unexpected column."""
    # Arrange
    valid_df_with_extra_col = valid_df.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs) -> DataFrame:
        return valid_df_with_extra_col

    monkeypatch.setattr(current_measurements_table, "_read", mock_read_table)

    # Act
    actual = current_measurements_table.read()

    # Assert
    assert actual.schema == ExternalDataProducts.CURRENT_MEASUREMENTS.schema
