import unittest
from pathlib import Path

import pytest
from dotenv import load_dotenv
from fixtures.eletrical_heating_fixture import ElectricalHeatingFixture

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

load_dotenv(f"{PROJECT_ROOT}/tests/.env")


@pytest.mark.skip(reason="This test is not ready to run in the CI/CD pipeline.")
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
        self.fixture.job_state.run_id = self.fixture.start_job(
            self.fixture.job_state.job_id, self.fixture.environment_configuration
        )
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__wait_for_job_to_complete(self):
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)
        assert self.fixture.job_state.run_result_state.value == "SUCCESS", (
            f"Job did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )
