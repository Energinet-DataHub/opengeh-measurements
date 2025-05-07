import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.catalog import Table

from tests import REQUIRED_DATABASES, SPARK_CATALOG_NAME


def test_table_properties(spark, migrations_executed):
    """
    Test the properties of tables in the specified databases after migration.
    This function creates the databases, runs the migration, and checks the properties of each table.
    """
    catalog = os.getenv("CATALOG_NAME", SPARK_CATALOG_NAME)
    validate_table_properties(spark=spark, databases=[f"{catalog}.{db}" for db in REQUIRED_DATABASES])


# TODO: Move the geh_common
def validate_table_properties(spark: SparkSession, databases: list[str] | None):
    tables: list[Table] = []
    if databases is None:  # TODO: Check that this is correct
        catalogDatabases = spark.catalog.listDatabases()
        databases = [f"{db.catalog}.{db.name}" for db in catalogDatabases]
    for db in databases:
        tables += spark.catalog.listTables(db)
    for table in tables:
        if table.tableType == "VIEW":
            continue
        fqn = f"{table.database}.{table.name}"
        assert table.tableType == "MANAGED", (
            f"Table {fqn}: expected table type to be 'MANAGED', got '{table.tableType}'"
        )
        logging.info(f"Testing Table: {fqn}")
        _test_table(spark, fqn)


# TODO: Move the geh_common
def _test_table(spark: SparkSession, fqn: str):
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
