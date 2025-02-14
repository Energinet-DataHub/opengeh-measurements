import os
from typing import Callable, Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from core.migrations import MigrationDatabaseNames, migrations_runner
from core.settings.catalog_settings import CatalogSettings


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = "app_conn_str"
    os.environ["CATALOG_NAME"] = "spark_catalog"
    os.environ["BRONZE_CONTAINER_NAME"] = "bronze"
    os.environ["SILVER_CONTAINER_NAME"] = "silver"
    os.environ["GOLD_CONTAINER_NAME"] = "gold"
    os.environ["BRONZE_DATABASE_NAME"] = "measurements_bronze"
    os.environ["SILVER_DATABASE_NAME"] = "measurements_silver"
    os.environ["GOLD_DATABASE_NAME"] = "measurements_gold"
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = "datalake"
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "false"


@pytest.fixture(scope="session")
def spark(tests_path: str) -> Generator[SparkSession, None, None]:
    warehouse_location = f"{tests_path}/__spark-warehouse__"

    session = configure_spark_with_delta_pip(
        SparkSession.builder.config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.sql.streaming.schemaInference", True)
        .config("spark.default.parallelism", 1)
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        .config("spark.shuffle.spill.compress", False)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", True)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={tests_path}/__metastore_db__;create=true",
        )
        .config(
            "javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("javax.jdo.option.ConnectionUserName", "APP")
        .config("javax.jdo.option.ConnectionPassword", "mine")
        .config("datanucleus.autoCreateSchema", "true")
        .config("hive.metastore.schema.verification", "false")
        .config("hive.metastore.schema.verification.record.version", "false")
        .enableHiveSupport(),
        extra_packages=[
            "org.apache.spark:spark-protobuf_2.12:3.5.4",
        ],
    ).getOrCreate()

    _create_schemas(session)

    yield session

    session.stop()


def _create_schemas(spark: SparkSession) -> None:
    catalog_settings = CatalogSettings()  # type: ignore
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MigrationDatabaseNames.measurements_internal_database}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_settings.bronze_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_settings.silver_database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_settings.gold_database_name}")


@pytest.fixture(scope="session")
def migrate(spark: SparkSession) -> None:
    """
    This is actually the main part of all our tests.
    The reason for being a fixture is that we want to run it only once per session.
    """
    migrations_runner.migrate()


@pytest.fixture(scope="session")
def file_path_finder() -> Callable[[str], str]:
    """
    Returns the path of the file.
    Please note that this only works if current folder haven't been changed prior using `os.chdir()`.
    The correctness also relies on the prerequisite that this function is actually located in a
    file located directly in the integration tests folder.
    """

    def finder(file: str) -> str:
        return os.path.dirname(os.path.normpath(file))

    return finder


@pytest.fixture(scope="session")
def source_path(file_path_finder: Callable[[str], str]) -> str:
    """
    Returns the <repo-root>/source folder path.
    Please note that this only works if current folder haven't been changed prior using `os.chdir()`.
    The correctness also relies on the prerequisite that this function is actually located in a
    file located directly in the integration tests folder.
    """
    return file_path_finder(f"{__file__}/../..")


@pytest.fixture(scope="session")
def tests_path(source_path: str) -> str:
    """
    Returns the <repo-root>/source folder path.
    Please note that this only works if current folder haven't been changed prior using `os.chdir()`.
    The correctness also relies on the prerequisite that this function is actually located in a
    file located directly in the integration tests folder.
    """
    return f"{source_path}/tests/silver"


@pytest.fixture(autouse=True)
def configure_dummy_logging() -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    from geh_common.telemetry.logging_configuration import configure_logging

    configure_logging(cloud_role_name="any-cloud-role-name", tracer_name="any-tracer-name")
