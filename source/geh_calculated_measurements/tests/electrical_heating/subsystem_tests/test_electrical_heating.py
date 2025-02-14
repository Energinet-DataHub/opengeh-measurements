import unittest

import pytest

from tests.electrical_heating.subsystem_tests.fixtures.eletrical_heating_fixture import ElectricalHeatingFixture


class TestElectricalHeating(unittest.TestCase):
    """
    Subsystem test that verfiies a Databricks electrical heating job runs successfully to completion.
    """

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self, electrical_heating_fixture: ElectricalHeatingFixture):
        TestElectricalHeating.fixture = electrical_heating_fixture

    @pytest.mark.order(1)
    def test__get_job_id(self):
        self.fixture.job_state.job_id = self.fixture.get_job_id()
        assert self.fixture.job_state.job_id is not None

    @pytest.mark.order(2)
    def test__start_job(self):
        self.fixture.job_state.run_id = self.fixture.start_job(self.fixture.job_state.job_id)
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__wait_for_job_to_complete(self):
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)
        assert self.fixture.job_state.run_result_state.value == "SUCCESS", (
            f"Job did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )
