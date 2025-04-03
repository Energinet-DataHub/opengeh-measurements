import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.electrical_heating.fixture import Fixture

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


class TestElectricalHeating(JobTest):
    """
    Test class for electrical heating.
    """

    @pytest.fixture(scope="class")
    def fixture(self) -> JobTestFixture:
        return Fixture(
            job_parameters=job_parameters,
        )
