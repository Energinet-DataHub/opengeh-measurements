import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from gold.domain.constants.database_names import DatabaseNames
from gold.domain.constants.table_names import TableNames
from gold.domain.schemas.gold_measurements import (
    measurements_gold_schema,
)


def test__migrations__should_create_bronze_measurements_table(spark: SparkSession, migrations_executed):
    # Assert
    gold_measurements = spark.table(f"{DatabaseNames.gold_database}.{TableNames.gold_measurements_table}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=measurements_gold_schema)
