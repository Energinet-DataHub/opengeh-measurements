import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest
from tests.subsystem_tests.net_consumption_group_6.fixture import Fixture

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.mark.skip(reason="Test is not implemented")
class TestNetConsumptionGroup6(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        return Fixture(job_parameters=job_parameters)
