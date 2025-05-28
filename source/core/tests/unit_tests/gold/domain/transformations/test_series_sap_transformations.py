import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.gold.domain.transformations.series_sap_transformations as sut
from core.gold.domain.schemas.gold_measurements_series_sap import gold_measurements_series_sap_schema
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__transform__should_match_measurements_series_sap_schema(spark: SparkSession) -> None:
    # Arrange
    silver_measurements = SilverMeasurementsBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(silver_measurements)

    # Assert
    assert_schemas.assert_schema(actual.schema, gold_measurements_series_sap_schema, ignore_nullability=True)


def test__transform__should_return_serie_seq_no_column_with_offset(spark: SparkSession) -> None:
    # Arrange
    expected_offset = 200_000_000_000
    silver_measurements = SilverMeasurementsBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(silver_measurements)

    # Assert
    test = actual_row = actual.collect()
    print(test)  # noqa: T201

    actual_row = actual.collect()[0]
    assert actual_row["serie_seq_no"] >= expected_offset
