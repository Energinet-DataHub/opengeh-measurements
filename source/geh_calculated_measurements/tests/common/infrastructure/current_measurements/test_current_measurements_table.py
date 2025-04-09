from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

import geh_common.data_products.measurements_core.measurements_gold.current_v1
from geh_calculated_measurements.common.infrastructure.current_measurements.current_meaurements_table import (
    CurrentMeasurementsTable,
)
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from tests.conftest import SPARK_CATALOG_NAME


@pytest.fixture(scope="module")
def current_measurements(spark: SparkSession) -> CurrentMeasurementsTable:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MeasurementsGoldDatabaseDefinition.DATABASE_NAME}")
    return CurrentMeasurementsTable(SPARK_CATALOG_NAME)


@pytest.fixture(scope="module")
def expected(spark: SparkSession) -> DataFrame:
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
        CurrentMeasurementsTable.schema,
    )


def test__current_measurements_read__has_correct_schema_and_records(
    current_measurements: CurrentMeasurementsTable,
    expected: DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    def mock_read() -> DataFrame:
        return expected

    monkeypatch.setattr(current_measurements, "read", mock_read)

    # Act
    actual = current_measurements.read()

    # Assert
    assert actual.collect() == expected.collect()
    assert actual.schema == expected.schema
    assert actual.schema == current_v1.schema
