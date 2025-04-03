import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

import core.gold.domain.transformations.receipts_transformation as sut
from core.receipts.domain.schemas.receipts_schema import receipts_schema
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder


def test__transform_silver_to_gold__should_match_gold_schema(spark: SparkSession) -> None:
    # Arrange
    df_gold = GoldMeasurementsBuilder(spark).add_row().build()

    # Act
    df_receipts = sut.transform(df_gold)

    # Assert
    assert_schemas.assert_schema(actual=df_receipts.schema, expected=receipts_schema, ignore_nullability=True)
