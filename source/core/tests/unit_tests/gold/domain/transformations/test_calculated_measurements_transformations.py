import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.gold.domain.transformations.calculated_measurements_transformations as sut
from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from tests.helpers.builders.calculated_builder import CalculatedMeasurementsBuilder


def test__transform_calculated_to_gold__should_match_gold_schema(spark: SparkSession) -> None:
    # Arrange
    df_silver = CalculatedMeasurementsBuilder(spark).add_row().build()

    # Act
    df_gold = sut.transform_calculated_to_gold(df_silver)

    # Assert
    assert_schemas.assert_schema(actual=df_gold.schema, expected=gold_measurements_schema, ignore_nullability=True)
