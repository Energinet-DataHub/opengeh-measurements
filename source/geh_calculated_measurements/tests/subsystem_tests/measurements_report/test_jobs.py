import uuid

import pytest

from geh_calculated_measurements.testing import JobTest, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


class TestNetConsumptionGroup6(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        # Construct fixture
        base_job_fixture = JobTestFixture(
            environment_configuration=config,
            job_name="MeasurementsReport",
            job_parameters=job_parameters,
        )

        return base_job_fixture

    # TODO Henrik: Reenable this test when seeding is fixed
    @pytest.mark.skip(reason="Reenable in PR #647 when seeding is fixed")
    def test__and_then_data_is_written_to_delta(self, fixture: JobTestFixture):
        pass
