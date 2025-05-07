import logging
from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql.catalog import Table

from geh_calculated_measurements.database_migrations.migrations_runner import _migrate
from tests import REQUIRED_DATABASES, SPARK_CATALOG_NAME


def test_table_properties(spark):
    """
    Test the properties of tables in the specified databases after migration.
    This function creates the databases, runs the migration, and checks the properties of each table.
    """
    _assert_migrations(
        spark=spark,
        migration_func=_migrate,
        databases=REQUIRED_DATABASES,
        spark_catalog_name=SPARK_CATALOG_NAME,
    )


# TODO: Move the geh_common
def _assert_migrations(
    spark: SparkSession,
    migration_func: Callable[[str], None],
    databases: list[str],
    spark_catalog_name: str = "spark_catalog",
):
    for db in databases:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    migration_func(spark_catalog_name)
    tables: list[Table] = []
    for db in databases:
        tables += spark.catalog.listTables(db)
    for table in tables:
        if table.tableType == "VIEW":
            continue
        fqn = f"{spark_catalog_name}.{table.database}.{table.name}"
        assert table.tableType == "MANAGED", f"Table {fqn}: expected table type to be 'MANAGED', got {table.tableType}"
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
    assert table_format == "delta", f"Table {fqn}: expected format to be 'delta', got {table_format}"
    assert table_location != "", f"Table {fqn}: expected location to be set, got {table_location}"
    assert table_retention == "interval 30 days", (
        f"Table {fqn}: expected retention to be 30 days, got {table_retention}"
    )
    assert len(table_clustering_columns) > 0, (
        f"Table {fqn}: expected clustering columns to be set, got {table_clustering_columns}"
    )
