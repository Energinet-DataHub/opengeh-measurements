import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from opengeh_gold.domain.schemas.gold_measurements import (
    gold_measurements_schema,
)
from opengeh_gold.infrastructure.config.table_names import TableNames
from opengeh_gold.infrastructure.settings.catalog_settings import CatalogSettings


def test__migrations__should_create_gold_measurements(spark: SparkSession, migrations_executed):
    # Arrange
    catalog_settings = CatalogSettings()  # type: ignore

    # Assert
    gold_measurements = spark.table(f"{catalog_settings.gold_database_name}.{TableNames.gold_measurements}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=gold_measurements_schema)
