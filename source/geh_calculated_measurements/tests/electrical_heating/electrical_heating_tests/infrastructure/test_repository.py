from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from geh_common.domain.types import NetSettlementGroup
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.infrastructure import (
    ElectricityMarketRepository,
)
from tests import SPARK_CATALOG_NAME
from tests.external_data_products import ExternalDataProducts

_TEST_FILES_FOLDER_PATH = str(Path(__file__).parent / "test_data")


@pytest.fixture(scope="module")
def valid_dataframe(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(
        [
            (
                "140000000000170201",
                NetSettlementGroup.NET_SETTLEMENT_GROUP_2,
                1,
                datetime(2022, 1, 1, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                datetime(2022, 1, 1, 1, tzinfo=ZoneInfo("Europe/Copenhagen")),
            ),
        ],
        schema=ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS.schema,
    )
    assert df.schema == ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS.schema
    return df


@pytest.fixture(scope="module")
def electricity_market_repository(spark: SparkSession) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(spark, SPARK_CATALOG_NAME)


def test__when_invalid_contract__raises_with_useful_message(
    valid_dataframe: DataFrame,
    electricity_market_repository: ElectricityMarketRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    invalid_df = valid_dataframe.drop(F.col("metering_point_id"))

    def mock_read_table(*args, **kwargs):
        return invalid_df

    monkeypatch.setattr(electricity_market_repository, "_read_table", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"The data source does not comply with the contract.*",
    ):
        # Act
        electricity_market_repository.read_consumption_metering_point_periods()


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    valid_dataframe: DataFrame,
    electricity_market_repository: ElectricityMarketRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the table can handle columns being added as it is defined to _not_ be a breaking change.
    The repository should return the data without the unexpected column."""
    # Arrange
    valid_df_with_extra_col = valid_dataframe.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs):
        return valid_df_with_extra_col

    monkeypatch.setattr(electricity_market_repository, "_read_table", mock_read_table)

    # Act
    actual = electricity_market_repository.read_consumption_metering_point_periods()

    # Assert
    assert actual.schema == ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS.schema
