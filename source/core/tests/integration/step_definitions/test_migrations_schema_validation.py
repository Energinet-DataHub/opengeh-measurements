import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, scenarios, then, when

from core.bronze.domain.schemas.invalid_submitted_transactions import invalid_submitted_transactions_schema
from core.bronze.domain.schemas.migrated_transactions import migrated_transactions_schema
from core.bronze.domain.schemas.submitted_transactions import submitted_transactions_schema
from core.bronze.domain.schemas.submitted_transactions_quarantined import submitted_transactions_quarantined_schema
from core.bronze.infrastructure.config import BronzeTableNames
from core.gold.domain.schemas.gold_measurements import gold_measurements_schema
from core.gold.domain.schemas.gold_measurements_series_sap import gold_measurements_series_sap_schema
from core.gold.infrastructure.config import GoldTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder
from tests.helpers.schemas.silver_measurements_schema import silver_measurements_schema

scenarios("../features/migrations_schema_validation.feature")


# Given steps


@given("a row for the silver measurements table where is_cancelled is null", target_fixture="null_data")
def _(spark: SparkSession):
    return SilverMeasurementsBuilder(spark).add_row(is_cancelled=None).build()  # type: ignore


# When steps


@when(parsers.parse("accessing the {table_name} table"), target_fixture="actual_schema")
def _(spark: SparkSession, table_name: str):
    table_map = {
        "silver measurements": f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}",
        "gold measurements": f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}",
        "gold measurements series SAP": f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements_series_sap}",
        "bronze migrated transactions": f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_migrated_transactions_table}",
        "bronze submitted transactions": f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}",
        "bronze invalid submitted transactions": f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_invalid_submitted_transactions}",
        "bronze submitted transactions quarantined": f"{BronzeSettings().bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_quarantined}",
    }

    fq_table_name = table_map.get(table_name)
    assert fq_table_name is not None, f"Unknown table name: '{table_name}'"

    return spark.table(fq_table_name).schema


@when("attempting to insert the row into the silver measurements table", target_fixture="insert_result")
def _(spark: SparkSession, null_data):
    settings = SilverSettings()
    table_name = f"{settings.silver_database_name}.{SilverTableNames.silver_measurements}"
    count_before = spark.read.table(table_name).count()

    threw_exception = False
    try:
        null_data.write.mode("append").format("delta").saveAsTable(table_name)
    except Exception:
        threw_exception = True

    count_after = spark.read.table(table_name).count()

    return {
        "exception_thrown": threw_exception,
        "count_before": count_before,
        "count_after": count_after,
    }


# Then steps


@then(parsers.parse("the table schema should match the expected {schema_type} schema"))
def _(actual_schema, schema_type: str):
    schema_map = {
        "silver measurements": silver_measurements_schema,
        "gold measurements": gold_measurements_schema,
        "gold measurements series SAP": gold_measurements_series_sap_schema,
        "bronze migrated transactions": migrated_transactions_schema,
        "bronze submitted transactions": submitted_transactions_schema,
        "bronze invalid submitted transactions": invalid_submitted_transactions_schema,
        "bronze quarantined transactions": submitted_transactions_quarantined_schema,
    }

    expected_schema = schema_map.get(schema_type)
    assert expected_schema is not None, f"Unknown schema type: '{schema_type}'"
    assert_schemas.assert_schema(actual=actual_schema, expected=expected_schema)


@then("the insert should raise an exception")
def _(insert_result):
    assert insert_result["exception_thrown"]


@then("the number of rows in the silver measurements table should remain unchanged")
def _(insert_result):
    assert insert_result["count_before"] == insert_result["count_after"]
