import os
import uuid
from unittest import mock

import pytest
from azure.monitor.query import LogsQueryResult, LogsQueryStatus
from databricks.sdk.service.jobs import Run, RunResultState, RunState
from databricks.sdk.service.sql import ResultData, StatementResponse

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

METERING_POINT_ID = "170000040000000201"
CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1

job_parameters = {
    "orchestration-instance-id": str(uuid.uuid4()),
    "calculation-month": CALCULATION_MONTH,
    "calculation-year": CALCULATION_YEAR,
}

dummy_env = {
    "CALCULATED_MEASUREMENTS_WAREHOUSE_ID": "dummy_warehouse_id",
    "SHARED_CATALOG_NAME": "dummy_catalog_name",
    "DATABRICKS_TOKEN": "dummy_databricks_token",
    "WORKSPACE_URL": "dummy_workspace_url",
    "SHARED_KEYVAULT_NAME": "dummy_shared_keyvault_name",
    "SHARED_SCHEMA_NAME": "dummy_shared_schema_name",
    "TIME_SERIES_POINTS_TABLE": "dummy_time_series_points_table",
    "CONSUMPTION_METERING_POINTS_TABLE": "dummy_consumption_metering_points_table",
    "CHILD_METERING_POINTS_TABLE": "dummy_child_metering_points_table",
}


def mock_init(self, *args, **kwargs):
    self.config = EnvironmentConfiguration()
    self.job_name = "CapacitySettlement"
    self.job_parameters = job_parameters
    self.run_id = 1

    self.databricks_api_client = mock.Mock()
    self.databricks_api_client.get_job_id.return_value = 1
    self.databricks_api_client.start_job.return_value = 1
    self.databricks_api_client.client.jobs.run_now_and_wait.return_value = Run(
        state=RunState(result_state=RunResultState.SUCCESS)
    )
    self.databricks_api_client.wait_for_job_completion.return_value = RunResultState.SUCCESS
    self.databricks_api_client.execute_statement.return_value = StatementResponse()
    self.databricks_api_client.execute_statement.return_value.result = ResultData(row_count=1)

    self.secret_client = mock.Mock()
    self.azure_logs_query_client = mock.Mock()
    self.azure_logs_query_client.wait_for_condition.return_value = LogsQueryResult(status=LogsQueryStatus.SUCCESS)


class DummyClass:
    pass


class TestRunnerWithCorrectImplementation(JobTester):
    @property
    def fixture(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(os, "environ", dummy_env)
            mp.setattr(JobTestFixture, "__init__", mock_init)
            mp.setattr(JobTestFixture, "start_job", lambda *args, **kwargs: 1)
            mp.setattr(JobTestFixture, "wait_for_job_to_completion", lambda *args, **kwargs: None)
            mp.setattr(JobTestFixture, "wait_for_log_query_completion", lambda *args, **kwargs: None)
            return JobTestFixture(
                environment_configuration=EnvironmentConfiguration(),
                job_name="CapacitySettlement",
                job_parameters=job_parameters,
            )

    @pytest.mark.order(999)
    def test_function_calls(self):
        self.fixture.azure_logs_query_client.wait_for_condition.called_once()
        self.fixture.databricks_api_client.execute_statement.called_once()
        self.fixture.databricks_api_client.client.jobs.run_now_and_wait.called_once()


def test_when_fixture_not_property__then_raise_exception():
    with pytest.raises(AssertionError, match="The fixture property must be of type property."):

        class TestRunnerWithFixtureNotProperty(JobTester):
            def fixture(self):
                pass


def test_when_no_fixture__then_raise_exception():
    with pytest.raises(AssertionError, match="The subclass must implement the fixture property."):

        class TestRunnerWithoutFixture(JobTester):
            pass


@pytest.mark.parametrize("fixture", [1, "string", 1.0, [], {}, DummyClass()])
def test_when_fixture_not_return_JobTestFixture__then_raise_exception(fixture):
    with pytest.raises(AssertionError, match="The fixture property must return an instance of JobTestFixture."):

        class TestRunnerWithFixtureNotJobTestFixture(JobTester):
            @property
            def fixture(self):
                return fixture
