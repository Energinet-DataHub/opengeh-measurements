"""Pytest configuration for measurements."""

# import pytest


# @pytest.fixture(scope="session")
# def spark():
#     """Create a spark session for testing.
#
#     NOTE: You need to install `pyspark` and the `delta-spark` package to run this fixture.
#     You can install them using the following command:
#     ```
#     uv add pyspark delta-spark
#     ```
#     """
#     from pathlib import Path
#
#     from delta import configure_spark_with_delta_pip
#     from pyspark.sql import SparkSession
#
#     app_name = "measurements-testing"
#     warehouse_location = Path(__file__).parent / ".spark-warehouse"
#     metastore_path = Path(__file__).parent / ".metastore_db"
#
#     spark_session_conf = (
#         SparkSession.builder.appName(app_name)
#         .enableHiveSupport()
#         .config("spark.sql.warehouse.dir", warehouse_location.as_uri())
#         .config("spark.sql.catalogImplementation", "hive")
#         .config(
#             "javax.jdo.option.ConnectionURL",
#             f"jdbc:derby:;databaseName={metastore_path};create=true",
#         )
#         .config(
#             "javax.jdo.option.ConnectionDriverName",
#             "org.apache.derby.jdbc.EmbeddedDriver",
#         )
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#         )
#     )
#     spark = configure_spark_with_delta_pip(spark_session_conf).getOrCreate()
#     yield spark
#     spark.stop()
