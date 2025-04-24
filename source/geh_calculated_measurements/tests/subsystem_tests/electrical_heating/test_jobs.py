import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.electrical_heating.seed_table import seed_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


class TestElectricalHeating(JobTest):
    """
    Test class for electrical heating.
    """

    @pytest.fixture(scope="class")
    def fixture(self, electricity_market_data_products_created_as_tables):
        config = EnvironmentConfiguration()
        fixture = JobTestFixture(
            environment_configuration=config,
            job_name="ElectricalHeating",
            job_parameters=job_parameters,
        )
        seed_table(fixture)
        return fixture

    # TODO Henrik: Reenable this test when seeding is fixed
    @pytest.mark.skip(reason="Reenable in PR #647 when seeding is fixed")
    def test__and_then_data_is_written_to_delta(self, fixture: JobTestFixture):
        pass
