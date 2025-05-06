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
        # (f"{SPARK_CATALOG_NAME}.measurements_calculated_internal.executed_migrations", ["col1", "col2"]),
        (
            f"{SPARK_CATALOG_NAME}.measurements_calculated_internal.capacity_settlement_ten_largest_quantities",
            ("orchestration_instance_id", "metering_point_id", "observation_time"),
        ),
        (
            f"{SPARK_CATALOG_NAME}.measurements_calculated_internal.capacity_settlement_calculations",
            ["orchestration_instance_id", "execution_time"],
        ),
        (
            f"{SPARK_CATALOG_NAME}.measurements_calculated_internal.calculated_measurements",
            ["orchestration_instance_id", "transaction_id", "metering_point_id", "transaction_creation_datetime"],
        ),
        # (
        #    f"{SPARK_CATALOG_NAME}.measurements_calculated_internal.missing_measurements_log",
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

    # arrange

    details_df = spark.sql(f"DESCRIBE DETAIL {fqn}")
    details = details_df.collect()[0].asDict()  # .get("type")

    location = details.get("location", "")

    actual_clustering_cols = details.get("clusteringColumns", [])
    expected_cluster_cols = [col.strip() for col in cluster_columns]

    # Check if liquid clustering is enabled
    assert actual_clustering_cols, f"Table {fqn} does not have liquid clustering enabled"

    # Check if cluster columns are set correct
    assert sorted(actual_clustering_cols) == sorted(expected_cluster_cols), (
        f"Table {fqn} clustering columns mismatch.\nExpected: {cluster_columns}\nActual: {actual_clustering_cols}"
    )

    # check if retention policy is 30 days
    retention = details["properties"].get("delta.deletedFileRetentionDuration")
    assert retention == "interval 30 days", f"Expected 30-day retention, but found {retention}"

    # check if table is managed
    assert "spark-warehouse" in location, f"table is not managed and is located at: {location}"
