import abc
import time
from datetime import timedelta

import pytest
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob, Run, RunResultState, Wait
from databricks.sdk.service.sql import StatementResponse, StatementState

from geh_calculated_measurements.common.infrastructure.calculated_measurements.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.testing.utilities.log_query_client_wrapper import (
    LogQueryClientWrapper,
    LogsQueryStatus,
)
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class JobTestFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration, job_name: str, job_parameters: dict = {}):
        self.config = environment_configuration
        self.ws = WorkspaceClient(host=self.config.workspace_url, token=self.config.databricks_token)
        self.job_name = job_name
        self.job_parameters = job_parameters

        # Configure Azure resources
        credentials = DefaultAzureCredential()
        self.azure_logs_query_client = LogQueryClientWrapper(credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{self.config.shared_keyvault_name}.vault.azure.net/",
            credential=credentials,
        )

    def _get_job_by_name(self, job_name: str) -> BaseJob:
        jobs = list(self.ws.jobs.list(name=job_name))
        if len(jobs) == 0:
            raise ValueError(f"No job found with name {job_name}.")
        if len(jobs) > 1:
            raise ValueError(f"Multiple jobs found with name {job_name}.")
        return jobs[0]

    def start_job(self) -> Wait[Run]:
        base_job = self._get_job_by_name(self.job_name)
        params = [f"--{key}={value}" for key, value in self.job_parameters.items()]
        job = self.ws.jobs.run_now(job_id=base_job.job_id, python_params=params)
        return job

    def wait_for_job_completion(self, job: Wait[Run], timeout: int = 15) -> RunResultState | None:
        run = job.result(timeout=timedelta(minutes=timeout))
        return run.state.result_state

    def run_job_and_wait(self) -> Run:
        base_job = self._get_job_by_name(self.job_name)
        params = [f"--{key}={value}" for key, value in self.job_parameters.items()]
        return self.ws.jobs.run_now_and_wait(job_id=base_job.job_id, python_params=params)

    def execute_statement(
        self, statement: str, timeout_minutes: int = 15, poll_interval_seconds: int = 5
    ) -> StatementResponse:
        response = self.ws.statement_execution.execute_statement(
            warehouse_id=self.config.warehouse_id, statement=statement
        )

        # Wait for the statement to complete
        start_time = time.time()
        elapsed_time = 0
        while elapsed_time < timeout_minutes * 60:
            response = self.ws.statement_execution.get_statement(response.statement_id)
            if response.status.state not in [StatementState.RUNNING, StatementState.PENDING, StatementState.SUCCEEDED]:
                raise ValueError(f"Statement execution failed with state {response.status.state}.")
            if response.status.state == StatementState.SUCCEEDED:
                return response
            elapsed_time = time.time() - start_time
            print(f"Query did not complete in {elapsed_time} seconds. Retrying in {poll_interval_seconds} seconds...")  # noqa: T201
            time.sleep(poll_interval_seconds)

    def wait_for_log_query_completion(self, query: str) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)


class JobTester(abc.ABC):
    @pytest.fixture(scope="class")
    @abc.abstractmethod
    def fixture(self) -> JobTestFixture:
        raise NotImplementedError("The fixture method must be implemented.")

    @pytest.mark.order(1)
    def test__fixture_is_correctly_made(self, fixture: JobTestFixture):
        assert fixture is not None, "The fixture was not created successfully."
        assert isinstance(fixture, JobTestFixture), "The fixture is not of the correct type."

    @pytest.mark.order(2)
    def test__job_completes_successfully(self, fixture: JobTestFixture):
        result = fixture.run_job_and_wait()
        assert result.state.result_state is not None, "The job did not return a RunResultState."
        assert result.state.result_state == RunResultState.SUCCESS, f"The job did not complete successfully: {result}"

    @pytest.mark.order(3)
    def test__and_then_job_telemetry_is_created(self, fixture: JobTestFixture):
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{fixture.job_parameters.get("orchestration-instance-id")}'
        """

        # Act
        actual = fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, (
            f"The query did not complete successfully: {actual.status}. Query: {query}"
        )

    @pytest.mark.order(4)
    def test__and_then_data_is_written_to_delta(self, fixture: JobTestFixture):
        # Arrange
        catalog = fixture.config.catalog_name
        database = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        table = "calculated_measurements"
        statement = f"""
            SELECT * 
            FROM {catalog}.{database}.{table} 
            WHERE orchestration_instance_id = '{fixture.job_parameters.get("orchestration-instance-id")}' 
            LIMIT 1
        """

        # Act
        response = fixture.execute_statement(statement=statement)

        # Assert
        row_count = response.result.row_count if response.result.row_count is not None else 0
        assert row_count > 0, (
            f"Expected count to be greater than 0 for table {catalog}.{database}.{table}, but got {row_count}."
        )
