import unittest
import uuid

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState

from tests.subsystem_tests.missing_measurements_log.fixtures.missing_measurements_log_fixture import (
    MissingMeasurementsLogFixture,
)


class TestMissingMeasurementsLog(unittest.TestCase):
    """
    Subsystem test that verifies a Databricks missing measurements log job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, missing_measurements_log_fixture: MissingMeasurementsLogFixture) -> None:
        TestMissingMeasurementsLog.fixture = missing_measurements_log_fixture

    @pytest.mark.order(1)
    @pytest.mark.skip(reason="Skipping until job is ready.")
    def test__given_job_input(self) -> None:
        # Act
        self.fixture.job_state.calculation_input.job_id = self.fixture.get_job_id()
        self.fixture.job_state.calculation_input.orchestration_instance_id = uuid.uuid4()

        # Assert
        assert self.fixture.job_state.calculation_input.job_id is not None

    @pytest.mark.order(2)
    @pytest.mark.skip(reason="Skipping until job is ready.")
    def test__when_job_is_started(self) -> None:
        # Act
        self.fixture.job_state.run_id = self.fixture.start_job(self.fixture.job_state.calculation_input)

        # Assert
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    @pytest.mark.skip(reason="Skipping until job is ready.")
    def test__then_job_is_completed(self) -> None:
        # Act
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)

        # Assert
        assert self.fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"The Job {self.fixture.job_state.calculation_input.job_id} did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    @pytest.mark.skip(reason="Skipping until job is ready.")
    def test__and_then_job_telemetry_is_created(self) -> None:
        # Arrange
        if self.fixture.job_state.run_result_state != RunResultState.SUCCESS:
            raise Exception("A previous test did not complete successfully.")

        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{self.fixture.job_state.calculation_input.orchestration_instance_id}'
        """

        # Act
        actual = self.fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"
