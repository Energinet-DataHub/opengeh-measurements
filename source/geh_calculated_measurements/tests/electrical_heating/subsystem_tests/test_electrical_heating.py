import unittest
import uuid

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState
from fixtures.eletrical_heating_fixture import ElectricalHeatingFixture


class TestElectricalHeating(unittest.TestCase):
    """
    Subsystem test that verifies a Databricks electrical heating job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, electrical_heating_fixture: ElectricalHeatingFixture) -> None:
        TestElectricalHeating.fixture = electrical_heating_fixture

    @pytest.mark.order(1)
    def test__given_job_input(self) -> None:
        # Act
        self.fixture.job_state.calculation_input.job_id = self.fixture.get_job_id()
        self.fixture.job_state.calculation_input.orchestrator_instance_id = uuid.uuid4()

        # Assert
        assert self.fixture.job_state.calculation_input.job_id is not None

    @pytest.mark.order(2)
    def test__when_job_started(self) -> None:
        # Act
        self.fixture.job_state.run_id = self.fixture.start_job(self.fixture.job_state.calculation_input)

        # Assert
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__then_job_is_completed(self) -> None:
        # Act
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)

        # Assert
        assert self.fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"The Job {self.fixture.job_state.calculation_input.job_id} did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    def test__and_then_job_logged(self) -> None:
        # Arrange
        if self.fixture.job_state.run_result_state != RunResultState.SUCCESS:
            raise Exception("A previous test did not complete successfully.")

        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration-instance-id"] == '{self.fixture.job_state.calculation_input.orchestrator_instance_id}'
        """

        # Act
        actual = self.fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"
