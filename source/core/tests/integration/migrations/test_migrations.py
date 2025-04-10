import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession

from core.bronze.domain.schemas.invalid_submitted_transactions import invalid_submitted_transactions_schema
from core.bronze.domain.schemas.migrated_transactions import migrated_transactions_schema
from core.bronze.domain.schemas.submitted_transactions import submitted_transactions_schema
from core.bronze.domain.schemas.submitted_transactions_quarantined import submitted_transactions_quarantined_schema
from core.bronze.infrastructure.config import BronzeTableNames
from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from core.gold.infrastructure.config import GoldTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder
from tests.helpers.schemas.silver_measurements_schema import silver_measurements_schema


def test__migrations__should_create_silver_measurements_table(spark: SparkSession) -> None:
    # Arrange
    silver_settings = SilverSettings()

    # Assert
    silver_measurements = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}")
    assert_schemas.assert_schema(actual=silver_measurements.schema, expected=silver_measurements_schema)


def test__migrations__should_create_gold_measurements(spark: SparkSession) -> None:
    # Arrange
    gold_settings = GoldSettings()

    # Assert
    gold_measurements = spark.table(f"{gold_settings.gold_database_name}.{GoldTableNames.gold_measurements}")
    assert_schemas.assert_schema(actual=gold_measurements.schema, expected=gold_measurements_schema)


def test__migrations__should_create_bronze_migrated_table(spark: SparkSession):
    # Arrange
    bronze_settings = BronzeSettings()

    # Assert
    bronze_migrated = spark.table(
        f"{bronze_settings.bronze_database_name}.{BronzeTableNames.bronze_migrated_transactions_table}"
    )
    assert_schemas.assert_schema(actual=bronze_migrated.schema, expected=migrated_transactions_schema)


def test__ingest_submitted_transactions__should_create_submitted_transactions_table(spark: SparkSession) -> None:
    # Arrange
    bronze_settings = BronzeSettings()

    # Assert
    submitted_transactions = spark.table(
        f"{bronze_settings.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}"
    )
    assert_schemas.assert_schema(actual=submitted_transactions.schema, expected=submitted_transactions_schema)


def test__migration__should_create_invalid_submitted_transactions_table(spark: SparkSession) -> None:
    # Arrange
    bronze_settings = BronzeSettings()

    # Assert
    invalid_submitted_transactions = spark.table(
        f"{bronze_settings.bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}"
    )
    assert_schemas.assert_schema(
        actual=invalid_submitted_transactions.schema, expected=invalid_submitted_transactions_schema
    )


def test__migration__should_add_is_cancelled_null_constraints(spark: SparkSession) -> None:
    # Arrange
    silver_settings = SilverSettings()
    silver_data_builder = SilverMeasurementsBuilder(spark)
    silver_data_builder.add_row(is_cancelled=None)

    null_data = silver_data_builder.build()
    act_area_threw_exception = False

    table_name = f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}"
    count_before = spark.read.table(table_name).count()

    # Act
    try:
        null_data.write.mode("append").format("delta").saveAsTable(table_name)
    except Exception:
        act_area_threw_exception = True

    count_after = spark.read.table(table_name).count()

    # Assert
    assert act_area_threw_exception
    assert count_before == count_after


def test__migration__should_create_submitted_transactions_quarantined_table(spark: SparkSession) -> None:
    # Arrange
    bronze_settings = BronzeSettings()

    # Assert
    submitted_transactions_quarantined = spark.table(
        f"{bronze_settings.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}"
    )
    assert_schemas.assert_schema(
        actual=submitted_transactions_quarantined.schema, expected=submitted_transactions_quarantined_schema
    )
