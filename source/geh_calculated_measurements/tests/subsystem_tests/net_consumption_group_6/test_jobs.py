import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_table import delete_seeded_data, seed_table

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


class TestNetConsumptionGroup6(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        # Construct fixture
        base_job_fixture = JobTestFixture(
            environment_configuration=config,
            job_name="NetConsumptionGroup6",
            job_parameters=job_parameters,
        )

        # Remove previously inserted seeded data
        delete_seeded_data(base_job_fixture)

        # Insert seeded data
        seed_table(base_job_fixture)

        yield base_job_fixture

        # Remove previously inserted seeded data
        delete_seeded_data(base_job_fixture)
