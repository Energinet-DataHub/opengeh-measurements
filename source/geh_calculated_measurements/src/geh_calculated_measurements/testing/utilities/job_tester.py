import abc

import pytest
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from geh_common.databricks.databricks_api_client import DatabricksApiClient

from geh_calculated_measurements.common.infrastructure.calculated_measurements.database_definitions import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.testing.utilities.log_query_client_wrapper import (
    LogQueryClientWrapper,
    LogsQueryStatus,
)
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class JobTestFixture:
    def __init__(
        self, environment_configuration: EnvironmentConfiguration, job_name: str, job_parameters: dict
    ) -> None:
        self.config = environment_configuration
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )

        self.credentials = DefaultAzureCredential()
        self.azure_logs_query_client = LogQueryClientWrapper(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )
        self.job_name = job_name
        self.job_parameters = job_parameters

    def set_run_id(self, run_id: int) -> None:
        self.run_id = run_id

    def get_run_id(self) -> int | None:
        return self.run_id

    def start_job(self) -> int:
        job_id = self.databricks_api_client.get_job_id(self.job_name)
        params_list = []
        if self.job_parameters:
            for key, value in self.job_parameters.items():
                params_list.append(f"--{key}={value}")

        run_id = self.databricks_api_client.start_job(job_id, params_list)

        return run_id

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(self, query: str) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)


class JobTester(abc.ABC):
    def __init_subclass__(cls):
        """Check that the subclass has implemented the fixture property."""
        assert hasattr(cls, "fixture"), "The subclass must implement the fixture property."
        assert isinstance(cls.fixture, property), (
            f"The fixture property must be of type property. Got: {type(cls.fixture)}"
        )
        fixture = cls.fixture.fget(cls)
        assert isinstance(fixture, JobTestFixture), (
            f"The fixture property must return an instance of JobTestFixture. Got: {type(fixture)}"
        )
        return super().__init_subclass__()

    @property
    @abc.abstractmethod
    def fixture(self) -> JobTestFixture:
        pass

    @pytest.mark.order(1)
    def test__when_job_is_started(self) -> None:
        # Arrange
        run_id = self.fixture.start_job()

        # Act
        self.fixture.set_run_id(run_id)

        # Assert
        assert self.fixture.get_run_id() is not None

    @pytest.mark.order(2)
    def test__then_job_is_completed(self) -> None:
        # Arrange
        run_id = self.fixture.get_run_id()
        if run_id is None:
            raise ValueError("run_id is None, cannot proceed with job completion check.")

        # Act
        run_result_state = self.fixture.wait_for_job_to_completion(run_id)

        # Assert
        assert run_result_state == RunResultState.SUCCESS, (
            f"The Job with run id {run_id} did not complete successfully: {run_result_state.value}"
        )

    @pytest.mark.order(3)
    def test__and_then_job_telemetry_is_created(self) -> None:
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{self.fixture.job_parameters.get("orchestration-instance-id")}'
        """

        # Act
        actual = self.fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, (
            f"The query did not complete successfully: {actual.status}. Query: {query}"
        )

    @pytest.mark.order(4)
    def test__and_then_data_is_written_to_delta(self) -> None:
        # Arrange
        catalog = self.fixture.config.catalog_name
        database = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        table = "calculated_measurements"
        statement = f"""
            SELECT * FROM {catalog}.{database}.{table} WHERE orchestration_instance_id = '{self.fixture.job_parameters.get("orchestration-instance-id")}' LIMIT 1
            """

        # Act
        response = self.fixture.databricks_api_client.execute_statement(
            warehouse_id=self.fixture.config.warehouse_id,
            statement=statement,
            wait_for_response=True,
        )

        # Assert
        row_count = response.result.row_count if response.result.row_count is not None else 0
        assert row_count > 0, (
            f"Expected count to be greater than 0 for table {catalog}.{database}.{table}, but got {row_count}."
        )
