import unittest

import pytest
from azure.monitor.query import LogsQueryStatus
from fixtures.eletrical_heating_fixture import ElectricalHeatingFixture

from geh_calculated_measurements.electrical_heating.domain import ColumnNames


class TestElectricalHeating(unittest.TestCase):
    """
    Subsystem test that verfiies a Databricks electrical heating job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, electrical_heating_fixture: ElectricalHeatingFixture):
        TestElectricalHeating.fixture = electrical_heating_fixture

    @pytest.mark.order(1)
    def test__given_job_input(self):
        # Arrange & Act
        self.fixture.job_state.job_id = self.fixture.get_job_id()

        # Assert
        assert self.fixture.job_state.job_id is not None

    @pytest.mark.order(2)
    def test__when_job_started(self):
        # Arrange & Act
        self.fixture.job_state.run_id = self.fixture.start_job(self.fixture.job_state.job_id)

        # Assert
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__then_job_is_completed(self):
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)
        assert self.fixture.job_state.run_result_state.value == "SUCCESS", (
            f"Job did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    def test__and_then_logged_to_application_insights(self):
        # Arrange
        query = f"""
        AppTraces 
        | where Properties["{ColumnNames.orchestration_instance_id}"] == '{self.fixture.job_state.orchestrator_instance_id}'
        """

        # Act
        actual = self.fixture.query_workspace_logs(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, (
            f"Log workspace query did not complete successfully: {actual.status}"
        )
        assert actual.tables.count > 0, "Log workspace query is empty."  # type: ignore
