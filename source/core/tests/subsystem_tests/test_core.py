# import time

# import pytest
# from pyspark.sql import SparkSession

# import tests.helpers.identifier_helper as identifier_helper
# from tests.helpers.builders.submitted_transactions_builder import ValueBuilder
# from tests.subsystem_tests.fixtures.core_fixture import CoreFixture
# from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


# def test__dummy_test(spark: SparkSession) -> None:
#     # Arrange
#     databricks_settings = DatabricksSettings()  # type: ignore
#     fixture = CoreFixture(databricks_settings)
#     value = ValueBuilder(spark).add_row().build()

#     # fixture.start_jobs()
#     fixture.send_submitted_transactions_event(value)

#     start_time = time.time()
#     timeout = 1000
#     poll_interval = 10

#     while time.time() - start_time < (timeout + 20):
#         # fixture.assert_runs_are_running()
#         fixture.assert_receipt()

#         if start_time - time.time() > timeout:
#             raise TimeoutError("Timeout error")

#         time.sleep(poll_interval)

#     # Send event to the eventhub
#     # Wait for a receipt (max 10 minutes) and assert that jobs are running
#     # Assert receipt

#     # Assert
