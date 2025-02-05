import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from silver.schemas.silver_measurements_schema import silver_measurements_schema

from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from core.gold.infrastructure.config import GoldDatabaseNames, GoldTableNames
from core.silver.infrastructure.config import SilverDatabaseNames, SilverTableNames


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrations_executed) -> None:
    # Assert
    silver_measurements = spark.table(f"{SilverDatabaseNames.silver}.{SilverTableNames.silver_measurements}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)


def test__migrations__should_create_gold_measurements(spark: SparkSession, migrations_executed) -> None:
    # Assert
    gold_measurements = spark.table(f"{GoldDatabaseNames.gold}.{GoldTableNames.gold_measurements}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=gold_measurements_schema)
