import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from opengeh_gold.domain.constants.database_names import DatabaseNames
from opengeh_gold.domain.constants.table_names import TableNames
from opengeh_gold.domain.schemas.gold_measurements import (
    gold_measurements_schema,
)


def test__migrations__should_create_gold_measurements_table(spark: SparkSession, migrations_executed):
    # Assert
    gold_measurements = spark.table(f"{DatabaseNames.gold_database}.{TableNames.gold_measurements_table}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=gold_measurements_schema)
