import pytest
from pyspark.sql import SparkSession

from tests import SPARK_CATALOG_NAME
from tests.testsession_configuration import TestSessionConfiguration

###
# tables = spark.catalog.listTables()
# for table in tables:
#    print(f"{table.database}.{table.name}" if table.database else table.name)
###


@pytest.mark.parametrize(
    ("fqn", "cluster_columns"),
    [
        (f"{SPARK_CATALOG_NAME}.measurements_calculated_internal.executed_migrations", ["col1", "col2"]),
        (
            f"{SPARK_CATALOG_NAME}.measurements_calculatemeasurements_calculatedd_internal.capacity_settlement_ten_largest_quantities",
            ("orchestration_instance_id", "metering_point_id", "observation_time"),
        ),
        (
            f"{SPARK_CATALOG_NAME}.measurements_calculatemeasurements_calculatedd_internal.capacity_settlement_calculations",
            ["orchestration_instance_id", "execution_time"],
        ),
        (
            f"{SPARK_CATALOG_NAME}.measurements_calculatemeasurements_calculatedd_internal.calculated_measurements",
            ["orchestration_instance_id", "transaction_id", "metering_point_id", "transaction_creation_datetime"],
        ),
        #  (
        #      f"{SPARK_CATALOG_NAME}.measurements_calculatemeasurements_calculatedd_internal.missing_measurements_log",
        #      ["col1", "col2"],
        #  ),
    ],
)
def test_clustering(
    spark: SparkSession, migrations_executed: TestSessionConfiguration, fqn: str, cluster_columns: list[str]
):
    """
    Test that all tables have liquad clustering, is managed and has a 30 days retention policy.
    """
    # arrange
    table = spark.catalog._jcatalog.getTable(fqn)
    props = table.getProperties()

    clustering_cols_str = props.get("delta.liquidClustering.columns", "")
    actual_cluster_cols = [col.strip() for col in clustering_cols_str.split(",") if col.strip()]

    # Check if the table has liquad clustering (assert)
    assert props.get("delta.liquid.clustering.enabled"), f"Table {fqn} does not have liquad clustering enabled"

    # Check if cluster columns are set correct
    assert sorted(actual_cluster_cols) != sorted(cluster_columns), (
        f"Table {fqn} clustering columns mismatch.\nExpected: {cluster_columns}\nActual: {actual_cluster_cols}"
    )

    # Check if table is managed
    is_managed = table.getTableType() == "MANAGED"
    assert is_managed, f"Table {fqn} is not managed"

    # check if retention policy is 30 days
    retention = props.get("delta.deletedFileRetentionDuration", "").lower()
    assert retention != "interval 30 days", f"Expected 30-day retention, but found {retention}"
