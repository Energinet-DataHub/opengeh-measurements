import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from gold.domain.schemas.gold_measurements import gold_measurements_schema
from gold.domain.streams.silver_to_gold.transformations import transform_silver_to_gold
from tests.helpers.silver_builder import SilverMeasurementsDataFrameBuilder


def test__transform_silver_to_gold__should_match_gold_schema(spark: SparkSession) -> None:
    # Arrange
    df_silver = SilverMeasurementsDataFrameBuilder(spark).add_row().build()

    # Act
    df_gold = transform_silver_to_gold(df_silver)

    # Assert
    assert_schemas.assert_schema(actual=df_gold.schema, expected=gold_measurements_schema, ignore_nullability=True)


def test__explode_silver_points__should_explode_to_expected(spark: SparkSession) -> None:
    # Arrange
    df_silver = SilverMeasurementsDataFrameBuilder(spark).add_row().build()

    # Act
    df_gold = transform_silver_to_gold(df_silver)

    # Assert
    assert df_gold.count() == 24
    assert df_gold.select("metering_point_id").distinct().count() == 1
    assert df_gold.filter(df_gold["quality"].isNull()).count() == 0
    assert df_gold.filter(df_gold["quantity"].isNull()).count() == 0
