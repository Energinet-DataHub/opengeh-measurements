from abc import abstractmethod

import pytest
from databricks.sdk.service.jobs import RunResultState
from geh_common.databricks import DatabricksApiClient

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.testing.utilities.log_query_client_wrapper import LogsQueryStatus
from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class BaseJobTests:
    @pytest.fixture(autouse=True, scope="class")
    @abstractmethod
    def setup_fixture() -> BaseJobFixture:
        """
        Abstract fixture method to be implemented by subclasses.

        This method is necessary due to the stateless nature of pytest, wherein the state is cleared between
        individual test executions. To address this limitation, this fixture can be called to retrieve the
        state between the tests. By setting the scope to `class`, we ensure that only the implementation
        subclass has access to the state.
        """
        pass

    @pytest.mark.order(1)
    def test__when_job_is_started(self, setup_fixture: BaseJobFixture) -> None:
        # Act
        run_id = setup_fixture.start_job()
        setup_fixture.set_run_id(run_id)

        # Assert
        assert setup_fixture.get_run_id() is not None

    @pytest.mark.order(2)
    def test__then_job_is_completed(self, setup_fixture: BaseJobFixture) -> None:
        # Act
        run_id = setup_fixture.get_run_id()
        if run_id is None:
            raise ValueError("run_id is None, cannot proceed with job completion check.")
        run_result_state = setup_fixture.wait_for_job_to_completion(run_id)

        # Assert
        assert run_result_state == RunResultState.SUCCESS, (
            f"The Job with run id {run_id} did not complete successfully: {run_result_state.value}"
        )

    @pytest.mark.order
    def test__and_then_job_telemetry_is_created(self, setup_fixture: BaseJobFixture) -> None:
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{setup_fixture.job_parameters.get("orchestration-instance-id")}'
        """

        # Act
        actual = setup_fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"

    @pytest.mark.order(5)
    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, setup_fixture: BaseJobFixture
    ) -> None:
        # Arrange
        databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )

        catalog = environment_configuration.catalog_name
        database = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        table = "calculated_measurements"
        statement = f"""
            SELECT * FROM {catalog}.{database}.{table} LIMIT 1
            """

        # Act
        response = databricks_api_client.execute_statement(
            warehouse_id=environment_configuration.warehouse_id,
            statement=statement,
            wait_for_response=True,
        )

        # Assert
        row_count = response.result.row_count if response.result.row_count is not None else 0
        assert row_count > 0, (
            f"Expected count to be greater than 0 for table {catalog}.{database}.{table}, but got {row_count}"
        )
