import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.net_consumption_group_6.seed_table import (
    _seed_gold_table,
)

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.mark.skip(reason="The test is failing because the seeded data lacks the date prior the calculation date.")
class TestNetConsumptionGroup6(JobTest):
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
        # TODO JMK: Views cannot be modified
        # delete_seeded_data(base_job_fixture)

        # Seed gold table
        _seed_gold_table(base_job_fixture)

        # Seed electricity market
        # TODO JMK: Views cannot be modified
        # seed_electricity_market_tables(base_job_fixture)

        return base_job_fixture

        # Remove previously inserted seeded data
        # TODO JMK: Views cannot be modified
        # delete_seeded_data(base_job_fixture)

    @pytest.mark.skip(reason="TODO JVM")
    def test__and_then_data_is_available_in_gold(self, fixture: JobTestFixture):
        pass
