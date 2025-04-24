from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
)
from geh_common.domain.types import MeteringPointResolution
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.missing_measurements_log.infrastructure import MeteringPointPeriodsTable
from tests import SPARK_CATALOG_NAME


@pytest.fixture(scope="module")
def valid_dataframe(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(
        [
            (
                "123456789012345",
                "804",
                MeteringPointResolution.HOUR.value,
                datetime(2022, 1, 1, 0, tzinfo=ZoneInfo("Europe/Copenhagen")),
                datetime(2022, 1, 1, 1, tzinfo=ZoneInfo("Europe/Copenhagen")),
            ),
        ],
        schema=MeteringPointPeriodsTable.schema,
    )
    assert df.schema == MeteringPointPeriodsTable.schema
    return df


@pytest.fixture(scope="module")
def metering_point_periods_table() -> MeteringPointPeriodsTable:
    return MeteringPointPeriodsTable(
        SPARK_CATALOG_NAME, missing_measurements_log_metering_point_periods_v1.database_name
    )


def test__when_invalid_contract__raises_with_useful_message(
    valid_dataframe: DataFrame,
    metering_point_periods_table: MeteringPointPeriodsTable,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    invalid_df = valid_dataframe.drop(F.col("metering_point_id"))

    def mock_read_table(*args, **kwargs):
        return invalid_df

    monkeypatch.setattr(metering_point_periods_table, "_read", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"The data source does not comply with the contract.*",
    ):
        # Act
        metering_point_periods_table.read()


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    valid_dataframe: DataFrame,
    metering_point_periods_table: MeteringPointPeriodsTable,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the table can handle columns being added as it is defined to _not_ be a breaking change.
    The repository should return the data without the unexpected column."""
    # Arrange
    valid_df_with_extra_col = valid_dataframe.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs):
        return valid_df_with_extra_col

    monkeypatch.setattr(metering_point_periods_table, "_read", mock_read_table)

    # Act
    actual = metering_point_periods_table.read()

    # Assert
    assert actual.schema == MeteringPointPeriodsTable.schema
