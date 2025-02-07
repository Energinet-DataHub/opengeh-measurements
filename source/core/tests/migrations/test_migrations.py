import testcommon.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from silver.schemas.silver_measurements_schema import silver_measurements_schema

from core.bronze.domain.schemas.bronze_measurements import bronze_measurements_schema
from core.bronze.domain.schemas.submitted_transactions import submitted_transactions_schema
from core.bronze.infrastructure.config import BronzeTableNames
from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from core.gold.infrastructure.config import GoldTableNames
from core.settings.catalog_settings import CatalogSettings
from core.silver.infrastructure.config import SilverTableNames


def test__migrations__should_create_silver_measurements_table(spark: SparkSession, migrations_executed: None) -> None:
    # Arrange
    catalog_settings = CatalogSettings()  # type: ignore

    # Assert
    silver_measurements = spark.table(f"{catalog_settings.silver_database_name}.{SilverTableNames.silver_measurements}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)


def test__migrations__should_create_gold_measurements(spark: SparkSession, migrations_executed: None) -> None:
    # Arrange
    catalog_settings = CatalogSettings()  # type: ignore

    # Assert
    gold_measurements = spark.table(f"{catalog_settings.gold_database_name}.{GoldTableNames.gold_measurements}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=gold_measurements_schema)


def test__migrations__should_create_bronze_measurements_table(spark: SparkSession, migrations_executed: None) -> None:
    # Arrange
    catalog_settings = CatalogSettings()  # type: ignore

    # Assert
    bronze_measurements = spark.table(
        f"{catalog_settings.bronze_database_name}.{BronzeTableNames.bronze_measurements_table}"
    )
    assert_schemas.assert_schema(actual=bronze_measurements.schema, expected=bronze_measurements_schema)


def test__ingest_submitted_transactions__should_create_submitted_transactions_table(
    spark: SparkSession, migrations_executed: None
) -> None:
    # Arrange
    catalog_settings = CatalogSettings()  # type: ignore

    # Assert
    submitted_transactions = spark.table(
        f"{catalog_settings.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}"
    )
    assert_schemas.assert_schema(actual=submitted_transactions.schema, expected=submitted_transactions_schema)
