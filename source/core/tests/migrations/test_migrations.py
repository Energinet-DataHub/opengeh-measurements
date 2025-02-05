import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from silver.schemas.silver_measurements_schema import silver_measurements_schema

from core.bronze.domain.constants import BronzeDatabaseNames, BronzeTableNames
from core.bronze.domain.schemas.bronze_measurements import bronze_measurements_schema
from core.bronze.domain.schemas.submitted_transactions import submitted_transactions_schema
from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from core.gold.infrastructure.config import GoldDatabaseNames, GoldTableNames
from core.silver.infrastructure.config import SilverDatabaseNames, SilverTableNames


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrations_executed: None) -> None:
    # Assert
    silver_measurements = spark.table(f"{SilverDatabaseNames.silver}.{SilverTableNames.silver_measurements}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)


def test__migrations__should_create_gold_measurements(spark: SparkSession, migrations_executed: None) -> None:
    # Assert
    gold_measurements = spark.table(f"{GoldDatabaseNames.gold}.{GoldTableNames.gold_measurements}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=gold_measurements_schema)


def test__migrations__should_create_bronze_measurements_table(spark: SparkSession, migrations_executed: None) -> None:
    # Assert
    bronze_measurements = spark.table(
        f"{BronzeDatabaseNames.bronze_database}.{BronzeTableNames.bronze_measurements_table}"
    )
    assert_schemas.assert_schema(actual=bronze_measurements.schema, expected=bronze_measurements_schema)


def test__ingest_submitted_transactions__should_create_submitted_transactions_table(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Assert
    submitted_transactions = spark.table(
        f"{BronzeDatabaseNames.bronze_database}.{BronzeTableNames.bronze_submitted_transactions_table}"
    )
    assert_schemas.assert_schema(actual=submitted_transactions.schema, expected=submitted_transactions_schema)
