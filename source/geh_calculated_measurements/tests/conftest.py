### This file contains the fixtures that are used in the tests. ###
import shutil
import sys
import tempfile
from typing import Generator
from unittest import mock

import geh_common.telemetry.logging_configuration as config
import pytest
from delta import configure_spark_with_delta_pip
from geh_common.testing.dataframes import AssertDataframesConfiguration, configure_testing
from pyspark import SparkConf
from pyspark.sql import SparkSession

from geh_calculated_measurements.database_migrations.migrations_runner import migrate
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
    conf = SparkConf().setAll(
        pairs=[
            (k, v)
            for k, v in {
                # Delta Lake configuration
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.catalogImplementation": "hive",
                "spark.sql.warehouse.dir": f"{data_dir}/spark-warehouse",
                "spark.local.dir": f"{data_dir}/spark-tmp",
                "javax.jdo.option.ConnectionURL": f"jdbc:derby:;databaseName={data_dir}/metastore;create=true",
                "javax.jdo.option.ConnectionUserName": "APP",
                "javax.jdo.option.ConnectionPassword": "mine",
                # Disable schema verification
                "hive.metastore.schema.verification": "false",
                "hive.metastore.schema.verification.record.version": "false",
                "datanucleus.autoCreateSchema": "true",
                # Disable the UI
                "spark.ui.showConsoleProgress": "false",
                "spark.ui.enabled": "false",
                # Optimize for small tests
                "spark.ui.dagGraph.retainedRootRDDs": "1",
                "spark.ui.retainedJobs": "1",
                "spark.ui.retainedStages": "1",
                "spark.ui.retainedTasks": "1",
                "spark.sql.ui.retainedExecutions": "1",
                "spark.worker.ui.retainedExecutors": "1",
                "spark.worker.ui.retainedDrivers": "1",
                "spark.databricks.delta.snapshotPartitions": "2",
                "spark.sql.shuffle.partitions": "1",
                "spark.driver.memory": "2g",
                "spark.executor.memory": "2g",
                "spark.sql.streaming.schemaInference": "true",
                "spark.rdd.compress": "false",
                "spark.shuffle.compress": "false",
                "spark.shuffle.spill.compress": "false",
                "spark.sql.session.timeZone": "UTC",
                "spark.driver.extraJavaOptions": f"-Ddelta.log.cacheSize=3 -Dderby.system.home={data_dir} -XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops",
            }.items()
        ]
    )
    builder = configure_spark_with_delta_pip(SparkSession.Builder().config(conf=conf).enableHiveSupport())
    return builder.master("local[1]").getOrCreate(), data_dir


_spark, data_dir = _get_spark()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session with Delta Lake enabled.
    """
    yield _spark
    _spark.stop()
    shutil.rmtree(data_dir)


@pytest.fixture(scope="session", autouse=True)
def fix_print():
    """
    pytest-xdist disables stdout capturing by default, which means that print() statements
    are not captured and displayed in the terminal.
    That's because xdist cannot support -s for technical reasons wrt the process execution mechanism
    https://github.com/pytest-dev/pytest-xdist/issues/354
    """
    original_print = print
    with mock.patch("builtins.print") as mock_print:
        mock_print.side_effect = lambda *args, **kwargs: original_print(*args, **{"file": sys.stderr, **kwargs})
        yield mock_print


@pytest.fixture(scope="session")
def test_session_configuration() -> TestSessionConfiguration:
    """Load the test session configuration from the testsession.local.settings.yml file.

    This is a useful feature for developers who wants to run the tests with different configurations
    on their local machine. The file is not included in the repository, so it's up to the developer to create it.
    """
    settings_file_path = TESTS_ROOT / "testsession.local.settings.yml"
    return TestSessionConfiguration.load(settings_file_path)


@pytest.fixture(scope="session", autouse=True)
def _configure_testing_decorator(test_session_configuration: TestSessionConfiguration) -> None:
    configure_testing(
        is_testing=test_session_configuration.scenario_tests.testing_decorator_enabled,
        rows=test_session_configuration.scenario_tests.testing_decorator_max_rows,
    )


@pytest.fixture(scope="session")
def assert_dataframes_configuration(
    test_session_configuration: TestSessionConfiguration,
) -> AssertDataframesConfiguration:
    """This fixture is used for comparing data frames in scenario tests.

    It's mainly specific to the scenario tests. The fixture is placed here to avoid code duplication."""
    return AssertDataframesConfiguration(
        show_actual_and_expected_count=test_session_configuration.scenario_tests.show_actual_and_expected_count,
        show_actual_and_expected=test_session_configuration.scenario_tests.show_actual_and_expected,
        show_columns_when_actual_and_expected_are_equal=test_session_configuration.scenario_tests.show_columns_when_actual_and_expected_are_equal,
    )


def _create_databases(spark: SparkSession) -> None:
    # """
    # Create Unity Catalog databases as they are not created by migration scripts.
    # They are created by infrastructure (in the real environments)
    # In tests they are created in the single available default catalog.
    # """
    spark.sql("CREATE DATABASE IF NOT EXISTS measurements_calculated")
    spark.sql("CREATE DATABASE IF NOT EXISTS measurements_calculated_internal")


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession) -> None:
    """Executes all migrations.

    This fixture is useful for all tests that require the migrations to be executed. E.g. when
    a view/dataprodcut/table is required."""
    _create_databases(spark)
    migrate()
