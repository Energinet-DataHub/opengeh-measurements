from abc import abstractmethod

import pytest


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

    # @pytest.mark.order(1)
    # def test__given_job_input(self, setup_fixture) -> None:
    #     # Act
    #     setup_fixture.create_calculation_input()

    #     # Assert
    #     assert setup_fixture.calculation_input.job_id is not None

    # @pytest.mark.order(2)
    # def test__when_job_is_started(self, setup_fixture) -> None:
    #     # Act
    #     run_id = setup_fixture.start_job(setup_fixture.calculation_input)
    #     setup_fixture.create_job_state(
    #         run_id=run_id, run_result_state=None, calculation_input=setup_fixture.calculation_input
    #     )

    #     # Assert
    #     assert setup_fixture.job_state.run_id is not None

    # @pytest.mark.order(3)
    # def test__then_job_is_completed(self, setup_fixture) -> None:
    #     # Act
    #     run_result_state = setup_fixture.wait_for_job_to_completion(setup_fixture.job_state.run_id)
    #     setup_fixture.set_run_result_state(run_result_state)

    #     # Assert
    #     assert setup_fixture.job_state.run_result_state == RunResultState.SUCCESS, (
    #         f"The Job {setup_fixture.calculation_input.job_id} did not complete successfully: {setup_fixture.job_state.run_result_state.value}"
    #     )

    # @pytest.mark.order(4)
    # def test__and_then_job_telemetry_is_created(self, setup_fixture) -> None:
    #     # Arrange
    #     if setup_fixture.job_state.run_result_state != RunResultState.SUCCESS:
    #         raise Exception("A previous test did not complete successfully.")

    #     query = f"""
    #     AppTraces
    #     | where Properties["Subsystem"] == 'measurements'
    #     | where Properties["orchestration_instance_id"] == '{setup_fixture.calculation_input.params.get("orchestration-instance-id")}'
    #     """

    #     # Act
    #     actual = setup_fixture.wait_for_log_query_completion(query)

    #     # Assert
    #     assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"

    # @pytest.mark.order(5)
    # def test__and_then_table_has_data0(self, setup_fixture, spark) -> None:
    #     catalog = CatalogSettings().catalog_name
    #     database = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
    #     table = "calculated_measurements"

    #     dataframe = spark.table(f"{catalog}.{database}.{table}").take(1)
    #     count = len(dataframe)

    #     assert count > 0, f"Expected count to be greater than 0 for table {catalog}.{database}.{table}, but got {count}"
