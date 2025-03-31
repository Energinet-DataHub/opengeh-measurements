import os
from typing import Callable, Generator
from unittest import mock
from unittest.mock import patch

import geh_common.telemetry.logging_configuration as config
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core
import core.gold.infrastructure.config.spark as gold_spark
import core.utility.shared_helpers as shared_helpers
import tests.helpers.environment_variables_helpers as environment_variables_helpers
import tests.helpers.schema_helper as schema_helper
from core.containers import Container
from core.migrations import migrations_runner


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    environment_variables_helpers.set_test_environment_variables()

    container = Container()
    container.init_resources()
    container.wire(packages=[core])


@pytest.fixture(scope="session")
def spark(tests_path: str, session_mocker: MockerFixture) -> Generator[SparkSession, None, None]:
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
    session_mocker.patch(f"{gold_spark.__name__}.initialize_spark", return_value=session)

    yield session

    session.stop()


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession, session_mocker: MockerFixture) -> None:
    """
    This is actually the main part of all our tests.
    The reason for being a fixture is that we want to run it only once per session.
    """
    session_mocker.patch.object(migrations_runner, migrations_runner.DatabricksApiClient.__name__)
    session_mocker.patch.object(migrations_runner, migrations_runner.DatabricksSettings.__name__)

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
    return file_path_finder(f"{__file__}/..")


@pytest.fixture(scope="session")
def tests_path(source_path: str) -> str:
    """
    Returns the <repo-root>/source folder path.
    Please note that this only works if current folder haven't been changed prior using `os.chdir()`.
    The correctness also relies on the prerequisite that this function is actually located in a
    file located directly in the integration tests folder.
    """
    return f"{source_path}/tests"


@pytest.fixture(scope="session")
def env_args_fixture_logging() -> dict[str, str]:
    env_args = {
        "CLOUD_ROLE_NAME": "test_role",
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "connection_string",
        "SUBSYSTEM": "test_subsystem",
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


@pytest.fixture(autouse=True)
def configure_dummy_logging(env_args_fixture_logging, script_args_fixture_logging):
    """Ensure that logging hooks don't fail due to _TRACER_NAME not being set."""

    with (
        mock.patch("sys.argv", script_args_fixture_logging),
        mock.patch.dict("os.environ", env_args_fixture_logging, clear=False),
        mock.patch(
            "geh_common.telemetry.logging_configuration.configure_azure_monitor"
        ),  # Patching call to configure_azure_monitor in order to not actually connect to app. insights.
    ):
        logging_settings = config.LoggingSettings()  # type: ignore
        yield config.configure_logging(logging_settings=logging_settings, extras=None)  # type: ignore


@pytest.fixture
def mock_checkpoint_path():
    with patch.object(
        shared_helpers,
        shared_helpers.get_storage_base_path.__name__,
        return_value="tests/__checkpoints__/",
    ) as mock_checkpoint_path:
        yield mock_checkpoint_path
