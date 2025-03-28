import pytest
from databricks.sdk.service.jobs import RunResultState

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.testing.utilities.log_query_client_wrapper import LogsQueryStatus
from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class BaseJobTests:
    @pytest.mark.order(1)
    def test__when_job_is_started(self, job_fixture: BaseJobFixture) -> None:
        # Act
        run_id = job_fixture.start_job()
        job_fixture.set_run_id(run_id)

        # Assert
        assert job_fixture.get_run_id() is not None

    @pytest.mark.order(2)
    def test__then_job_is_completed(self, job_fixture: BaseJobFixture) -> None:
        # Act
        run_id = job_fixture.get_run_id()
        if run_id is None:
            raise ValueError("run_id is None, cannot proceed with job completion check.")
        run_result_state = job_fixture.wait_for_job_to_completion(run_id)

        # Assert
        assert run_result_state == RunResultState.SUCCESS, (
            f"The Job with run id {run_id} did not complete successfully: {run_result_state.value}"
        )

    @pytest.mark.order(3)
    def test__and_then_job_telemetry_is_created(self, job_fixture: BaseJobFixture) -> None:
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{job_fixture.job_parameters.get("orchestration-instance-id")}'
        """

        # Act
        actual = job_fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, (
            f"The query did not complete successfully: {actual.status}. Query: {query}"
        )

    @pytest.mark.order(4)
    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, job_fixture: BaseJobFixture
    ) -> None:
        # Arrange
        catalog = environment_configuration.catalog_name
        database = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        table = "calculated_measurements"
        statement = f"""
            SELECT * FROM {catalog}.{database}.{table} 
            WHERE orchestration_instance_id = '{job_fixture.job_parameters.get("orchestration-instance-id")}' 
            LIMIT 1
        """

        # Act
        response = job_fixture.execute_statement(statement)

        # Assert
        row_count = getattr(response.result, "row_count", 0)
        assert row_count > 0, (
            f"Expected count to be greater than 0 for table {catalog}.{database}.{table}, but got {row_count}."
        )
