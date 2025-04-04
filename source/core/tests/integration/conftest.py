import pytest
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

from core.migrations import migrations_runner
from tests.helpers.schema_helper import create_external_schemas, create_external_tables


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
def create_external_resources(spark: SparkSession, session_mocker: MockerFixture) -> None:
    create_external_schemas(spark)
    create_external_tables(spark)
