import uuid

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState

from tests.subsystem_tests.base_resources.subsystem_test_fixture import SubsystemTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class TestElectricalHeating:
    """
    Verifies a job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, environment_configuration: EnvironmentConfiguration) -> None:
        TestElectricalHeating.fixture = SubsystemTestFixture(environment_configuration)

    @pytest.mark.order(1)
    def test__seed_gold_table(self) -> None:
        # Arrange
        TestElectricalHeating.fixture.seed_gold_table(TestElectricalHeating.fixture.get_gold_table_rows())

    @pytest.mark.order(2)
    def test__given_job_input(self) -> None:
        # Act
        TestElectricalHeating.fixture.job_state.input.job_id = TestElectricalHeating.fixture.get_job_id(
            "ElectricalHeating"
        )
        TestElectricalHeating.fixture.job_state.input.orchestration_instance_id = uuid.uuid4()
        job_parameters = {
            "orchestration-instance-id": TestElectricalHeating.fixture.job_state.input.orchestration_instance_id,
        }

        TestElectricalHeating.fixture.job_state.input.job_parameters = job_parameters

        # Assert
        assert TestElectricalHeating.fixture.job_state.input.job_id is not None

    @pytest.mark.order(3)
    def test__when_job_started(self) -> None:
        # Act
        TestElectricalHeating.fixture.job_state.run_id = TestElectricalHeating.fixture.start_job(
            TestElectricalHeating.fixture.job_state.input
        )

        # Assert
        assert TestElectricalHeating.fixture.job_state.run_id is not None

    @pytest.mark.order(4)
    def test__then_job_is_completed(self) -> None:
        # Act
        TestElectricalHeating.fixture.job_state.run_result_state = (
            TestElectricalHeating.fixture.wait_for_job_completion(TestElectricalHeating.fixture.job_state.run_id)
        )

        # Assert
        assert TestElectricalHeating.fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"The job {TestElectricalHeating.fixture.job_state.input.job_id} did not complete successfully: {TestElectricalHeating.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(5)
    def test__and_then_job_logged(self) -> None:
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{TestElectricalHeating.fixture.job_state.input.orchestration_instance_id}'
        """

        # Act
        actual = TestElectricalHeating.fixture.wait_for_log_query_completion(
            query, TestElectricalHeating.fixture.job_state
        )

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}."
