import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from geh_common.data_products.measurements_core.measurements_gold.current_v1 import current_v1
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.current_meaurements import (
    CurruntMeasurements,
)
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)


@pytest.fixture(scope="module")
def current_measurements(spark: SparkSession) -> CurruntMeasurements:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MeasurementsGoldDatabaseDefinition.DATABASE_NAME}")
    return CurruntMeasurements()


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
    return df


def test__(
    current_measurements: CurruntMeasurements,
    valid_df: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    def mock_read() -> DataFrame:
        return valid_df

    monkeypatch.setattr(current_measurements, "read", mock_read)

    # Act
    actual = current_measurements.read()

    # Assert
    assert actual.collect() == valid_df.collect()
    assert actual.schema == valid_df.schema
    assert actual.schema == current_v1
