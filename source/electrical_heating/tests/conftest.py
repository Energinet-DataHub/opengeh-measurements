from pathlib import Path
from typing import Generator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, Row
from testcommon.dataframes import assert_schema

from electrical_heating.infrastructure.measurements_bronze.schemas.measurements_bronze_v1 import point, measurements_bronze_v1
from electrical_heating.infrastructure.measurements_bronze.database_definitions import MeasurementsBronzeDatabase
import pyspark.sql.functions as F

from testsession_configuration import TestSessionConfiguration
from utils.delta_table_utils import create_delta_table, read_from_csv, write_dataframe_to_table
from telemetry_logging.logging_configuration import configure_logging

from tests import PROJECT_ROOT
from tests.testsession_configuration import (
    TestSessionConfiguration,
)

@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    yield
    # Clear the cache after each test module to avoid memory issues
    spark.catalog.clearCache()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = SparkSession.builder.appName("testcommon").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def configure_dummy_logging() -> None:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""

    configure_logging(
        cloud_role_name="any-cloud-role-name", tracer_name="any-tracer-name"
    )


@pytest.fixture(scope="session")
def tests_path() -> str:
    """Returns the tests folder path."""
    return (PROJECT_ROOT / "tests").as_posix()


@pytest.fixture(scope="session")
def contracts_path() -> str:
    """Returns the source/contract folder path."""
    return (PROJECT_ROOT / "contracts").as_posix()


@pytest.fixture(scope="session")
def test_session_configuration(tests_path: str) -> TestSessionConfiguration:
    settings_file_path = Path(tests_path) / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


@pytest.fixture(scope="session")
def test_files_folder_path(tests_path: str) -> str:
    return f"{tests_path}/test_files"


@pytest.fixture(scope="session")
def write_to_delta(spark: SparkSession, test_files_folder_path: str) -> None:

    create_delta_table(
        spark,
        database_name=MeasurementsBronzeDatabase.DATABASE_NAME,
        table_name=MeasurementsBronzeDatabase.MEASUREMENTS_NAME,
        schema=measurements_bronze_v1,
        table_location=f"test1/{MeasurementsBronzeDatabase.DATABASE_NAME}/{MeasurementsBronzeDatabase.MEASUREMENTS_NAME}",
    )

    file_name = f"{test_files_folder_path}/{MeasurementsBronzeDatabase.DATABASE_NAME}-{MeasurementsBronzeDatabase.MEASUREMENTS_NAME}.csv"
    measurements = read_from_csv(spark, file_name, schema=measurements_bronze_v1)

    measurements.show()
    measurements = measurements.withColumn(
        "points",
        F.from_json(F.col("points"), ArrayType(point)),
    )
    measurements = measurements.withColumn(
        "start_datetime",
        F.to_timestamp(F.col("start_datetime"), "yyyy-MM-dd HH:mm:ss"),
    ).withColumn(
        "end_datetime",
        F.to_timestamp(F.col("end_datetime"), "yyyy-MM-dd HH:mm:ss"))

    measurements = spark.createDataFrame(measurements.rdd, schema=measurements_bronze_v1, verifySchema=True)

    measurements.show()

    print(measurements_bronze_v1)
    measurements.printSchema()

    assert_schema(measurements.schema, measurements_bronze_v1)

    write_dataframe_to_table(
        df = spark.createDataFrame([Row("a")], schema=measurements_bronze_v1),
        database_name=MeasurementsBronzeDatabase.DATABASE_NAME,
        table_name=MeasurementsBronzeDatabase.MEASUREMENTS_NAME,
    )
