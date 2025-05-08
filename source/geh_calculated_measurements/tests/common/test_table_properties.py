import logging

from pyspark.sql import SparkSession
from pyspark.sql.catalog import Table

from tests import REQUIRED_DATABASES, SPARK_CATALOG_NAME


def test_table_properties(spark, migrations_executed):
    """Test that the properties of the tables follow the general requirements for tables managed using Spark/Databricks."""
    assert_table_properties(
        spark=spark,
        databases=[f"{SPARK_CATALOG_NAME}.{db}" for db in REQUIRED_DATABASES],
        excluded_tables=["executed_migrations"],
    )


# TODO: Move the geh_common
def assert_table_properties(
    spark: SparkSession, databases: list[str] | None = None, excluded_tables: list[str] | None = None
):
    """Assert table properties for all tables in the given databases.

    Asserts that the properties of the tables follow the general requirements defined
    in <ADD_LINK_TO_CONFLUENCE>.
    """
    tables: list[Table] = []
    if databases is None:  # TODO: Check that this is correct
        catalogDatabases = spark.catalog.listDatabases()
        databases = [f"{db.catalog}.{db.name}" for db in catalogDatabases]
        logging.info(f"Databases: {databases}")
    for db in databases:
        tables += spark.catalog.listTables(db)
    for table in tables:
        _assert_table(spark, table, excluded_tables=excluded_tables)


# TODO: Move the geh_common
def _assert_table(spark: SparkSession, table: Table, excluded_tables: list[str] | None = None):
    if table.tableType == "VIEW" or table.name in excluded_tables:
        return
    fqn = f"{table.database}.{table.name}"
    logging.info(f"Testing Table: {fqn}")
    assert table.tableType == "MANAGED", f"Table {fqn}: expected table type to be 'MANAGED', got '{table.tableType}'"
    details = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
    table_format = details.get("format", "")
    table_location = details.get("location", "")
    table_clustering_columns = details.get("clusteringColumns", [])
    table_properties = details.get("properties", {})
    table_retention = table_properties.get("delta.deletedFileRetentionDuration", "")
    assert table_format == "delta", f"Table {fqn}: expected format to be 'delta', got '{table_format}'"
    assert table_location != "", f"Table {fqn}: expected location to be set, got '{table_location}'"
    assert table_retention == "interval 30 days", (
        f"Table {fqn}: expected retention to be 30 days, got '{table_retention}'"
    )
    assert len(table_clustering_columns) > 0, (
        f"Table {fqn}: expected clustering columns to be set, got '{table_clustering_columns}'"
    )
