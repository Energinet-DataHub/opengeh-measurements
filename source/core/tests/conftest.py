from typing import Generator
from unittest import mock
from unittest.mock import patch

import geh_common.telemetry.logging_configuration as config
import pytest
from geh_common.testing.spark.spark_test_session import get_spark_test_session
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core.gold.infrastructure.config.spark as gold_spark
import core.utility.shared_helpers as shared_helpers
import tests.helpers.environment_variables_helpers as environment_variables_helpers
import tests.helpers.schema_helper as schema_helper
from core.migrations import migrations_runner


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    environment_variables_helpers.set_test_environment_variables()


@pytest.fixture(scope="session")
def spark(session_mocker: MockerFixture) -> Generator[SparkSession, None, None]:
    session, _ = get_spark_test_session()
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


@pytest.fixture
def mock_checkpoint_path():
    with patch.object(
        shared_helpers,
        shared_helpers.get_storage_base_path.__name__,
        return_value="tests/__checkpoints__/",
    ) as mock_checkpoint_path:
        yield mock_checkpoint_path
