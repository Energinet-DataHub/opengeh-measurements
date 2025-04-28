from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from geh_common.domain.types import MeteringPointSubType, MeteringPointType, NetSettlementGroup
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.electrical_heating.infrastructure import (
    ElectricityMarketRepository,
)
from tests import SPARK_CATALOG_NAME
from tests.external_data_products import ExternalDataProducts


@pytest.fixture(scope="module")
def electricity_market_repository(spark: SparkSession) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(spark, SPARK_CATALOG_NAME)


class TestReadConsumptionMeteringPointPeriods:
    @pytest.fixture(scope="module")
    def valid_dataframe(self, spark: SparkSession) -> DataFrame:
        df = spark.createDataFrame(
            [
                (
                    "170000000000000201",
                    NetSettlementGroup.NET_SETTLEMENT_GROUP_2.value,
                    1,
                    datetime(2023, 12, 31, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                    None,
                ),
            ],
            schema=ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS.schema,
        )
        assert df.schema == ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS.schema
        return df

    def test__when_invalid_contract__raises_with_useful_message(
        self,
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
        self,
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


class TestReadChildMeteringPoints:
    @pytest.fixture(scope="module")
    def valid_dataframe(self, spark: SparkSession) -> DataFrame:
        df = spark.createDataFrame(
            [
                (
                    "140000000000170201",
                    MeteringPointType.ELECTRICAL_HEATING,
                    MeteringPointSubType.CALCULATED,
                    "170000000000000201",
                    datetime(2023, 12, 31, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                    None,
                ),
            ],
            schema=ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS.schema,
        )
        assert df.schema == ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS.schema
        return df

    def test__when_invalid_contract__raises_with_useful_message(
        self,
        valid_dataframe: DataFrame,
        electricity_market_repository: ElectricityMarketRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        invalid_df = valid_dataframe.drop(F.col("metering_point_id"))

<<<<<<< HEAD
def test__when_consumption_missing_expected_column_raises_exception(spark: SparkSession, monkeypatch) -> None:
    with pytest.raises(ValueError, match=r"Column settlement_month not found in CSV"):
        _create_repository_with_missing_col(spark).read_consumption_metering_point_periods()
=======
        def mock_read_table(*args, **kwargs):
            return invalid_df
>>>>>>> 7addf24b0f0bbbc82030b5cbc179aa71b6cf7e3f

        monkeypatch.setattr(electricity_market_repository, "_read_table", mock_read_table)

        # Assert
        with pytest.raises(
            Exception,
            match=r"The data source does not comply with the contract.*",
        ):
            # Act
            electricity_market_repository.read_child_metering_points()

    def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
        self,
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
        actual = electricity_market_repository.read_child_metering_points()

        # Assert
        assert actual.schema == ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS.schema
