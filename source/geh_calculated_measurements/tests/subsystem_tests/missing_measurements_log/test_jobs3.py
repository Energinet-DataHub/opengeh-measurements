import uuid

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState

from tests.subsystem_tests.base_resources.subsystem_test_fixture import SubsystemTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class TestMissingMeasurementsLog:
    """
    Verifies a job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, environment_configuration: EnvironmentConfiguration) -> None:
        TestMissingMeasurementsLog.fixture = SubsystemTestFixture(environment_configuration)

    @pytest.mark.order(1)
    def test__given_job_input(self) -> None:
        # Act
        TestMissingMeasurementsLog.fixture.job_state.input.job_id = self.fixture.get_job_id("MissingMeasurementsLog")
        TestMissingMeasurementsLog.fixture.job_state.input.orchestration_instance_id = uuid.uuid4()
        job_parameters = {
            "orchestration-instance-id": TestMissingMeasurementsLog.fixture.job_state.input.orchestration_instance_id,
        }

        TestMissingMeasurementsLog.fixture.job_state.input.job_parameters = job_parameters

        # Assert
        assert TestMissingMeasurementsLog.fixture.job_state.input.job_id is not None

    @pytest.mark.order(2)
    def test__when_job_started(self) -> None:
        # Act
        TestMissingMeasurementsLog.fixture.job_state.run_id = TestMissingMeasurementsLog.fixture.start_job(
            TestMissingMeasurementsLog.fixture.job_state.input
        )

        # Assert
        assert TestMissingMeasurementsLog.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__then_job_is_completed(self) -> None:
        # Act
        self.fixture.job_state.run_result_state = TestMissingMeasurementsLog.fixture.wait_for_job_completion(
            TestMissingMeasurementsLog.fixture.job_state.run_id
        )

        # Assert
        assert TestMissingMeasurementsLog.fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"The job {TestMissingMeasurementsLog.fixture.job_state.input.job_id} did not complete successfully: {TestMissingMeasurementsLog.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    def test__and_then_job_logged(self) -> None:
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{TestMissingMeasurementsLog.fixture.job_state.input.orchestration_instance_id}'
        """

        # Act
        actual = TestMissingMeasurementsLog.fixture.wait_for_log_query_completion(
            query, TestMissingMeasurementsLog.fixture.job_state
        )

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}."
