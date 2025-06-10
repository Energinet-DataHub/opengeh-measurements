import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.gold.domain.transformations.sap_series_transformations as sut
from core.gold.domain.schemas.gold_measurements_sap_series import gold_measurements_sap_series_schema
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__transform__should_match_measurements_series_sap_schema(spark: SparkSession) -> None:
    # Arrange
    silver_measurements = SilverMeasurementsBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(silver_measurements)

    # Assert
    assert_schemas.assert_schema(actual.schema, gold_measurements_sap_series_schema, ignore_nullability=True)
