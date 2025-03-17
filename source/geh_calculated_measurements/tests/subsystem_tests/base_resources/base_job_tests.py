from abc import abstractmethod

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState


class BaseJobTests:
    @pytest.fixture(autouse=True, scope="class")
    @abstractmethod
    def job_fixture():
        """
        Abstract fixture method to be implemented by subclasses.

        This method is necessary due to the stateless nature of pytest, wherein the state is cleared between
        individual test executions. To address this limitation, this fixture can be called to retrieve the
        state between the tests. By setting the scope to `class`, we ensure that only the implementation
        subclass has access to the state.
        """
        pass

    @pytest.mark.order(1)
    def test__given_job_input(self, job_fixture) -> None:
        # Act
        job_fixture.create_calculation_input()

        # Assert
        assert job_fixture.calculation_input.job_id is not None

    @pytest.mark.order(2)
    def test__when_job_is_started(self, job_fixture) -> None:
        # Act
        run_id = job_fixture.start_job(job_fixture.calculation_input)
        job_fixture.create_job_state(
            run_id=run_id, run_result_state=None, calculation_input=job_fixture.calculation_input
        )

        # Assert
        assert job_fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__then_job_is_completed(self, job_fixture) -> None:
        # Act
        run_result_state = job_fixture.wait_for_job_to_completion(job_fixture.job_state.run_id)
        job_fixture.set_run_result_state(run_result_state)

        # Assert
        assert job_fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"The Job {job_fixture.calculation_input.job_id} did not complete successfully: {job_fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    def test__and_then_job_telemetry_is_created(self, job_fixture) -> None:
        # Arrange
        if job_fixture.job_state.run_result_state != RunResultState.SUCCESS:
            raise Exception("A previous test did not complete successfully.")

        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{job_fixture.calculation_input.params.get("orchestration-instance-id")}'
        """

        # Act
        actual = job_fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"
