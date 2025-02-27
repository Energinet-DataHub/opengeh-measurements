import pytest
from pyspark.sql import SparkSession

from tests.subsystem_tests.fixtures.core_fixture import CoreFixture
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


def test__dummy_test(spark: SparkSession) -> None:
    # Arrange
    databricks_settings = DatabricksSettings()  # type: ignore
    fixture = CoreFixture(databricks_settings)

    # fixture.start_jobs()

    fixture.send_submitted_transactions_event(spark)

    # Send event to the eventhub
    # Wait for a receipt (max 10 minutes) and assert that jobs are running
    # Assert receipt

    # Assert
    assert True is True


@pytest.mark.order(1)
def test__start_jobs() -> None:
    # Arrange
    databricks_settings = DatabricksSettings()  # type: ignore
    fixture = CoreFixture(databricks_settings)

    # Act
    fixture.start_jobs()

    # Assert
