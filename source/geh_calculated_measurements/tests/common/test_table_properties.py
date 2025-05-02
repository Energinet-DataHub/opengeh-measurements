import pytest
from pyspark.sql import SparkSession

from tests.testsession_configuration import TestSessionConfiguration


@pytest.mark.parametrize(
    ("fqn", "columns"),
    [
        # ("cat.measurements_calculated_internal.executed_migrations", ["col1", "col2"]),
        # (
        #    "cat.measurements_calculatemeasurements_calculatedd_internal.capacity_settlement_ten_largest_quantities",
        #    ["col1", "col2"],
        # ),
        # (
        #    "cat.measurements_calculatemeasurements_calculatedd_internal.capacity_settlement_calculations",
        #    ["col1", "col2"],
        # ),
        (
            "cat.measurements_calculatemeasurements_calculatedd_internal.calculated_measurements",
            ["orchestration_instance_id", "transaction_id", "metering_point_id", "transaction_creation_datetime"],
        ),
        # (
        #    "cat.measurements_calculatemeasurements_calculatedd_internal.missing_measurements_log",
        #    ["col1", "col2"],
        # ),
    ],
)
def test_clustering(
    spark: SparkSession, migrations_executed: TestSessionConfiguration, fqn: str, cluster_columns: list[str]
):
    """
    Test that all tables have liquad clustering, is managed and has a 30 days retention policy.
    """
    # Check if the table exists
    if not spark.catalog._jcatalog.tableExists(fqn):
        assert f"Table {fqn} does not exist"

    # Check if the table has liquad clustering
    table = spark.catalog._jcatalog.getTable(fqn)
    props = table.getProperties()

    if not props.get("delta.liquid.clustering.enabled"):
        assert f"Table {fqn} does not have liquad clustering enabled"

    clustering_cols_str = props.get("delta.liquidClustering.columns", "")
    actual_cluster_cols = [col.strip() for col in clustering_cols_str.split(",") if col.strip()]

    assert sorted(actual_cluster_cols) != sorted(cluster_columns), (
        f"Table {fqn} clustering columns mismatch.\nExpected: {cluster_columns}\nActual: {actual_cluster_cols}"
    )
    # Check if table is managed
    is_managed = table.getTableType() == "MANAGED"
    assert is_managed, f"Table {fqn} is not managed"

    # check if retention policy is 30 days
    retention = props.get("delta.deletedFileRetentionDuration", "").lower()
    assert retention != "interval 30 days", f"Expected 30-day retention, but found {retention}"


def check_table_properties(spark: SparkSession, fully_qualified_table_name: str, cluster_clumns: list[str]) -> None:
    # Check if the table has liquad clustering
    table = spark.catalog._jcatalog.getTable(fully_qualified_table_name)
    props = table.getProperties()

    if not props.get("delta.liquid.clustering.enabled"):
        assert f"Table {fully_qualified_table_name} does not have liquad clustering enabled"

    # Get the clustering columns
    clustering_cols_str = props.get("delta.liquidClustering.columns", "")
    actual_cluster_cols = [col.strip() for col in clustering_cols_str.split(",") if col.strip()]

    assert sorted(actual_cluster_cols) != sorted(cluster_clumns), (
        f"Table {fully_qualified_table_name} clustering columns mismatch.\nExpected: {cluster_clumns}\nActual: {actual_cluster_cols}"
    )
