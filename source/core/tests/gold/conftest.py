import os
from typing import Callable, Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import tests.helpers.schema_helper as schema_helper
from core.settings.silver_settings import SilverSettings
from core.silver.domain.schemas.silver_measurements import silver_measurements_schema
from core.silver.infrastructure.config import SilverTableNames


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
            "org.apache.hadoop:hadoop-azure:3.3.2",
            "org.apache.hadoop:hadoop-common:3.3.2",
            "io.delta:delta-spark_2.12:3.1.0",
            "io.delta:delta-core_2.12:2.3.0",
        ],
    ).getOrCreate()

    schema_helper.create_schemas(session)

    yield session

    session.stop()


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
    return f"{source_path}/tests/gold"


@pytest.fixture(scope="session")
def create_silver_tables(spark: SparkSession) -> None:
    silver_settings = SilverSettings()

    create_table_from_schema(
        spark=spark,
        database=silver_settings.silver_database_name,
        table_name=SilverTableNames.silver_measurements,
        schema=silver_measurements_schema,
    )


def create_table_from_schema(
    spark: SparkSession,
    database: str,
    table_name: str,
    schema: StructType,
    enable_change_data_feed: bool = False,
) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    schema_df = spark.createDataFrame([], schema=schema)
    ddl = schema_df._jdf.schema().toDDL()

    sql_command = f"CREATE TABLE IF NOT EXISTS {database}.{table_name} ({ddl}) USING DELTA"

    tbl_properties = []

    if enable_change_data_feed:
        tbl_properties.append("delta.enableChangeDataFeed = true")

    if len(tbl_properties) > 0:
        tbl_properties_string = ", ".join(tbl_properties)
        sql_command += f" TBLPROPERTIES ({tbl_properties_string})"

    spark.sql(sql_command)
