from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from geh_common.domain.types import MeteringPointResolution
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.missing_measurements_log.infrastructure import (
    MeteringPointPeriodsRepository,
)
from tests import SPARK_CATALOG_NAME
from tests.external_data_products import ExternalDataProducts


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
        schema=ExternalDataProducts.MISSING,
    )
    assert df.schema == ExternalDataProducts.MissingMeasurementsLogMeteringPointPeriods.schema
    return df


@pytest.fixture(scope="module")
def metering_point_periods_repository(spark: SparkSession) -> MeteringPointPeriodsRepository:
    return MeteringPointPeriodsRepository(spark, SPARK_CATALOG_NAME)


def test__when_invalid_contract__raises_with_useful_message(
    valid_dataframe: DataFrame,
    metering_point_periods_repository: MeteringPointPeriodsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    invalid_df = valid_dataframe.drop(F.col("metering_point_id"))

    def mock_read_table(*args, **kwargs):
        return invalid_df

    monkeypatch.setattr(metering_point_periods_repository, "_read_table", mock_read_table)

    # Assert
    with pytest.raises(
        Exception,
        match=r"The data source does not comply with the contract.*",
    ):
        # Act
        metering_point_periods_repository.read_metering_point_periods()


def test__when_source_contains_unexpected_columns__returns_data_without_unexpected_column(
    valid_dataframe: DataFrame,
    metering_point_periods_repository: MeteringPointPeriodsRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the table can handle columns being added as it is defined to _not_ be a breaking change.
    The repository should return the data without the unexpected column."""
    # Arrange
    valid_df_with_extra_col = valid_dataframe.withColumn("extra_col", F.lit("extra_value"))

    def mock_read_table(*args, **kwargs):
        return valid_df_with_extra_col

    monkeypatch.setattr(metering_point_periods_repository, "_read_table", mock_read_table)

    # Act
    actual = metering_point_periods_repository.read_metering_point_periods()

    # Assert
    assert actual.schema == ExternalDataProducts.MissingMeasurementsLogMeteringPointPeriods.schema


# TODO JMG : Contract test
