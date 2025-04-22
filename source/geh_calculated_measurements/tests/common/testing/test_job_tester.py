import os
import uuid
from unittest import mock

import pytest
from azure.monitor.query import LogsQueryResult, LogsQueryStatus
from databricks.sdk.service.jobs import Run, RunResultState, RunState, Wait
from databricks.sdk.service.sql import ResultData, StatementResponse, StatementState, StatementStatus

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
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
    self.azure_log_analytics_workspace_id = "dummy_workspace_id"

    self.ws = mock.Mock()
    self.ws.jobs.list.return_value = [mock.Mock()]
    self.ws.jobs.run_now.return_value = Wait(
        lambda *args, **kwargs: Run(state=RunState(result_state=RunResultState.SUCCESS))
    )
    self.ws.jobs.run_now_and_wait.return_value = Run(state=RunState(result_state=RunResultState.SUCCESS))
    self.ws.statement_execution.get_statement.return_value = StatementResponse(
        result=ResultData(row_count=1), status=StatementStatus(state=StatementState.SUCCEEDED)
    )

    self.secret_client = mock.Mock()
    self.azure_logs_query_client = mock.Mock()

    class MockTables:
        rows = [1, 2, 3, 4, 5]

    class MockResponse:
        status = LogsQueryStatus.SUCCESS
        tables = [MockTables(), MockTables()]

    self.azure_logs_query_client.wait_for_condition.return_value = LogsQueryResult(status=LogsQueryStatus.SUCCESS)
    self.azure_logs_query_client.query_workspace.return_value = MockResponse()


class TestRunnerWithCorrectImplementation(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(os, "environ", dummy_env)
            mp.setattr(JobTestFixture, "__init__", mock_init)
            mp.setattr(JobTestFixture, "start_job", lambda *args, **kwargs: 1)
            mp.setattr(JobTestFixture, "wait_for_job_completion", lambda *args, **kwargs: None)
            mp.setattr(JobTestFixture, "wait_for_log_query_completion", lambda *args, **kwargs: None)
            mp.setattr(JobTestFixture, "orchestration_instance_id", str(uuid.uuid4()))
            mp.setattr(
                JobTestFixture,
                "execute_statement",
                lambda *args, **kwargs: StatementResponse(status=StatementStatus(state=StatementState.SUCCEEDED)),
            )
            config = EnvironmentConfiguration()
            return JobTestFixture(
                environment_configuration=config,
                job_name="CapacitySettlement",
                job_parameters=job_parameters,
            )
