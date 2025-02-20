### This file contains the fixtures that are used in the tests. ###
import os
import shutil
from pathlib import Path
from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark import SparkConf
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.database_definitions import (
    CalculatedMeasurementsDatabase,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


def _remove_test_storage(spark_warehouse_dir: Path, metastore_db_dir: Path):
    try:
        if spark_warehouse_dir.exists():
            shutil.rmtree(spark_warehouse_dir)
    except Exception:
        print("Failed to remove the Spark warehouse directory.")  # noqa
    try:
        if metastore_db_dir.exists():
            shutil.rmtree(metastore_db_dir)
    except Exception:
        print("Failed to remove the metastore database directory.")  # noqa


@pytest.fixture(scope="session")
def spark(worker_id, tmp_path_factory) -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    app_name = "geh_calculated_measurements"

    spark_conf = {
        "spark.sql.session.timeZone": "UTC",
        # Warehouse configuration
        "spark.sql.warehouse.dir": tmp_path_factory.mktemp("spark_warehouse"),
        "spark.sql.shuffle.partitions": "1",
        "hive.stats.jdbc.timeout": "30",
        "hive.stats.retries.wait": "3000",
        # Performance configuration
        "spark.databricks.delta.snapshotPartitions": "2",
        "spark.driver.extraJavaOptions": f"-Ddelta.log.cacheSize=3 -Dderby.system.home={tmp_path_factory.mktemp('derby')} -XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops",
        "spark.ui.enabled": "False",
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.dagGraph.retainedRootRDDs": "1",
        "spark.ui.retainedJobs": "1",
        "spark.ui.retainedStages": "1",
        "spark.ui.retainedTasks": "1",
        "spark.sql.ui.retainedExecutions": "1",
        "spark.worker.ui.retainedExecutors": "1",
        "spark.worker.ui.retainedDrivers": "1",
        "spark.driver.memory": "2g",
    }

    conf = SparkConf().setAll(list(spark_conf.items()))
    spark_session = configure_spark_with_delta_pip(
        SparkSession.Builder().appName(app_name).config(conf=conf).enableHiveSupport()
    ).getOrCreate()
    schemas = [
        CalculatedMeasurementsDatabase.DATABASE_NAME,
        MeasurementsGoldDatabase.DATABASE_NAME,
    ]
    for schema in schemas:
        spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    yield spark_session
    spark_session.stop()


# https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
def pytest_collection_modifyitems(config, items) -> None:
    env_file_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_file_path):
        skip_subsystem_tests = pytest.mark.skip(
            reason="Skipping subsystem tests because .env file is missing. See .sample.env for an example."
        )
        for item in items:
            if "subsystem_tests" in item.nodeid:
                item.add_marker(skip_subsystem_tests)
