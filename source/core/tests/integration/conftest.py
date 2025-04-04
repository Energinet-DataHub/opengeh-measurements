import pytest
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import tests.helpers.environment_variables_helpers as environment_variables_helpers
import tests.helpers.schema_helper as schema_helper
from core.migrations import migrations_runner
from tests.helpers.schema_helper import create_external_schemas, create_external_tables


def pytest_runtest_setup() -> None:
    """
    This function is called before each test function is executed.
    """
    environment_variables_helpers.set_test_environment_variables()


@pytest.fixture(scope="session")
def migrations_executed(spark: SparkSession, session_mocker: MockerFixture) -> None:
    """
    This is actually the main part of all our tests.
    The reason for being a fixture is that we want to run it only once per session.
    """
    session_mocker.patch.object(migrations_runner, migrations_runner.DatabricksApiClient.__name__)
    session_mocker.patch.object(migrations_runner, migrations_runner.DatabricksSettings.__name__)

    schema_helper.create_internal_schemas(spark)

    migrations_runner.migrate()


@pytest.fixture(scope="session")
def create_external_resources(spark: SparkSession, session_mocker: MockerFixture) -> None:
    create_external_schemas(spark)
    create_external_tables(spark)
