import unittest

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState
from fixtures.eletrical_heating_fixture import ElectricalHeatingFixture


class TestElectricalHeating(unittest.TestCase):
    """
    Subsystem test that verifies a Databricks electrical heating job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, electrical_heating_fixture: ElectricalHeatingFixture):
        TestElectricalHeating.fixture = electrical_heating_fixture

    @pytest.mark.order(1)
    def test__given_job_input(self):
        # Act
        self.fixture.job_state.job_id = self.fixture.get_job_id()

        # Assert
        assert self.fixture.job_state.job_id is not None

    @pytest.mark.order(2)
    def test__when_job_started(self):
        # Act
        self.fixture.job_state.run_id = self.fixture.start_job(self.fixture.job_state.job_id)

        # Assert
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__then_job_is_completed(self):
        # Act
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)

        # Assert
        assert self.fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"Job did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    def test__and_then_logged_to_application_insights(self):
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements' 
        | where Properties["orchestration-instance-id"] == '{self.fixture.job_state.orchestrator_instance_id}'
        """

        # Act
        actual = self.fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"Query did not complete successfully: {actual.status}"
        assert len(actual.tables[0].rows) > 0, "Query is empty."  # type: ignore
