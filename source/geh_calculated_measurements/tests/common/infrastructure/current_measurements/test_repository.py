import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from geh_common.data_products.measurements_core.measurements_gold import current_v1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure import CurrentMeasurementsRepository
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


@pytest.fixture(scope="module")
def current_measurements_repository(spark: SparkSession) -> CurrentMeasurementsRepository:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MeasurementsGoldDatabaseDefinition.DATABASE_NAME}")
    return CurrentMeasurementsRepository(
        spark=spark,
        catalog_name=spark.catalog.currentCatalog(),
    )


@pytest.fixture(scope="module")
def valid_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(
        [
            (
                "123456789012345678",
                datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                Decimal("1.123"),
                "measured",
                "consumption",
            )
        ],
        CurrentMeasurements.schema,
    )
    assert df.schema == current_v1.schema
    return df


def test__when_invalid_contract__raises_with_useful_message(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    invalid_df = valid_df.drop(F.col("quantity"))

    def mock_read_table(*args, **kwargs):
        return invalid_df

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"The data source does not comply with the contract.*",
    ):
        # Act
        current_measurements_repository.read_current_measurements()


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    current_measurements_repository: CurrentMeasurementsRepository,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the repository can handle columns being added as it is defined to _not_ be a breaking change.
    The repository should return the data without the unexpected column."""
    # Arrange
    valid_df_with_extra_col = valid_df.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs):
        return valid_df_with_extra_col

    monkeypatch.setattr(CurrentMeasurementsRepository, "_read", mock_read_table)

    # Act
    actual = current_measurements_repository.read_current_measurements()

    # Assert
    assert actual.df.schema == current_v1.schema
