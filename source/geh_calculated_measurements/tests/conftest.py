### This file contains the fixtures that are used in the tests. ###
import atexit
import logging
import os
import shutil
import sys
import tempfile
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration as config
import pytest
from delta import configure_spark_with_delta_pip
from geh_common.testing.dataframes import configure_testing
from pyspark import SparkConf
from pyspark.sql import SparkSession

from tests import TESTS_ROOT
from tests.testsession_configuration import TestSessionConfiguration


@pytest.fixture(scope="session")
def env_args_fixture_logging() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
        "CATALOG_NAME": "spark_catalog",
    }
    return env_args


@pytest.fixture(scope="session")
def script_args_fixture_logging() -> list[str]:
    sys_argv = [
        "program_name",
        "--orchestration-instance-id",
        "00000000-0000-0000-0000-000000000001",
    ]
    return sys_argv


@pytest.fixture(scope="session", autouse=True)
def configure_dummy_logging(env_args_fixture_logging, script_args_fixture_logging) -> Generator[None, None, None]:
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""
    with (
        mock.patch("sys.argv", script_args_fixture_logging),
        mock.patch.dict("os.environ", env_args_fixture_logging, clear=False),
        mock.patch(
            "geh_common.telemetry.logging_configuration.configure_azure_monitor"
        ),  # Patching call to configure_azure_monitor in order to not actually connect to app. insights.
    ):
        logging_settings = config.LoggingSettings()
        yield config.configure_logging(logging_settings=logging_settings, extras=None)


@pytest.fixture(scope="module", autouse=True)
def clear_cache(spark: SparkSession) -> Generator[None, None, None]:
    """
    Clear the cache after each test module to avoid memory issues.
    """
    yield
    spark.catalog.clearCache()


def _get_spark():
    data_dir = tempfile.mkdtemp()
    spark_conf = SparkConf().setAll(
        [
            (k, v)
            for k, v in {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.catalogImplementation": "hive",
                "spark.sql.shuffle.partitions": "10",
                "spark.databricks.delta.snapshotPartitions": "2",
                "spark.driver.extraJavaOptions": f"-Ddelta.log.cacheSize=3 -XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops -Dderby.system.home={data_dir}",
                "spark.executor.extraJavaOptions": f"-Ddelta.log.cacheSize=3 -XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops -Dderby.system.home={data_dir}",
                "spark.ui.showConsoleProgress": "false",
                "spark.ui.enabled": "false",
                "spark.ui.dagGraph.retainedRootRDDs": "1",
                "spark.ui.retainedJobs": "1",
                "spark.ui.retainedStages": "1",
                "spark.ui.retainedTasks": "1",
                "spark.sql.ui.retainedExecutions": "1",
                "spark.worker.ui.retainedExecutors": "1",
                "spark.worker.ui.retainedDrivers": "1",
                "spark.driver.memory": "2g",
                "spark.executor.memory": "2g",
                "spark.executor.cores": "1",
                "spark.sql.warehouse.dir": f"{data_dir}/warehouse",
                "spark.sql.session.timeZone": "UTC",
                "javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={data_dir}/__metastore_db__;create=true",
                "javax.jdo.option.ConnectionDriverName": "org.apache.derby.jdbc.EmbeddedDriver",
                "javax.jdo.option.ConnectionUserName": "APP",
                "javax.jdo.option.ConnectionPassword": "mine",
                "datanucleus.autoCreateSchema": "true",
                "hive.metastore.schema.verification": "false",
                "hive.metastore.schema.verification.record.version": "false",
            }.items()
        ]
    )
    session = configure_spark_with_delta_pip(
        SparkSession.Builder().master("local").config(conf=spark_conf).enableHiveSupport()
    ).getOrCreate()
    session.sparkContext.setCheckpointDir(tempfile.mkdtemp())
    return session, data_dir


_spark, _data_dir = _get_spark()
atexit.register(lambda: shutil.rmtree(_data_dir, ignore_errors=True))


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    yield _spark
    _spark.stop()


@pytest.fixture(scope="session")
def test_session_configuration() -> TestSessionConfiguration:
    settings_file_path = TESTS_ROOT / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


@pytest.fixture(scope="session", autouse=True)
def configure_testing_decorator(
    test_session_configuration: TestSessionConfiguration,
) -> None:
    configure_testing(
        is_testing=test_session_configuration.scenario_tests.testing_decorator_enabled,
        rows=test_session_configuration.scenario_tests.testing_decorator_max_rows,
    )


def pytest_configure(config):
    """Write logs to a file in the logs directory

    pytest-xdist does not support '-s' option, so we need to write logs to a file.
    """
    logs_dir = TESTS_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is not None:
        logging.basicConfig(
            format=config.getini("log_file_format"),
            filename=logs_dir / f"tests_{worker_id}.log",
            level=config.getini("log_file_level"),
        )
        sys.stdout = open(logs_dir / f"tests_{worker_id}.out", "w")
