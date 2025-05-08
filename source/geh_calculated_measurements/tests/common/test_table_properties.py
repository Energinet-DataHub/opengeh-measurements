from geh_common.testing.dataframes.assert_table import assert_table_properties

from tests import REQUIRED_DATABASES, SPARK_CATALOG_NAME


def test_table_properties(spark, migrations_executed):
    """Test that the properties of the tables follow the general requirements for tables managed using Spark/Databricks."""
    assert_table_properties(
        spark=spark,
        databases=[f"{SPARK_CATALOG_NAME}.{db}" for db in REQUIRED_DATABASES],
        excluded_tables=["executed_migrations"],
    )
