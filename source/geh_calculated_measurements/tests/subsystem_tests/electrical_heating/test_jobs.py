import uuid

import pytest

from geh_calculated_measurements.testing import JobTest, JobTestFixture
from tests.subsystem_tests.electrical_heating.seed_table import seed_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


class TestElectricalHeating(JobTest):
    """
    Test class for electrical heating.
    """

    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        fixture = JobTestFixture(
            environment_configuration=config,
            job_name="ElectricalHeating",
            job_parameters=job_parameters,
        )
        seed_table(fixture, job_parameters.get("orchestration-instance-id"))
        return fixture
