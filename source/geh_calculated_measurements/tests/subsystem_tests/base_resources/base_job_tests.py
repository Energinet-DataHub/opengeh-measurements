from abc import abstractmethod

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture


class BaseJobTests:
    @pytest.fixture(autouse=True, scope="class")
    @abstractmethod
    def setup_fixture():
        """
        Abstract fixture method to be implemented by subclasses.

        This method is necessary due to the stateless nature of pytest, wherein the state is cleared between
        individual test executions. To address this limitation, this fixture can be called to retrieve the
        state between the tests. By setting the scope to `class`, we ensure that only the implementation
        subclass has access to the state.
        """
        pass

    @pytest.mark.order(1)
    def test__when_job_is_started(self, setup_fixture: BaseJobFixture) -> None:
        # Act
        run_id = setup_fixture.start_job()
        setup_fixture.set_run_id(run_id)

        # Assert
        assert setup_fixture.get_run_id() is not None

    @pytest.mark.order(2)
    def test__then_job_is_completed(self, setup_fixture: BaseJobFixture) -> None:
        # Act
        run_id = setup_fixture.get_run_id()
        if run_id is None:
            raise ValueError("run_id is None, cannot proceed with job completion check.")
        run_result_state = setup_fixture.wait_for_job_to_completion(run_id)

        # Assert
        assert run_result_state == RunResultState.SUCCESS, (
            f"The Job with run id {run_id} did not complete successfully: {run_result_state.value}"
        )

    @pytest.skip("This test is skipped due to issues with the telemetry data not being available in the logs.")
    @pytest.mark.order(3)
    def test__and_then_job_telemetry_is_created(self, setup_fixture: BaseJobFixture) -> None:
        # Arrange
        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{setup_fixture.job_parameters.get("orchestration-instance-id")}'
        """

        # Act
        actual = setup_fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"
