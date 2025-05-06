import uuid

import pytest

from geh_calculated_measurements.testing import JobTest, JobTestFixture
from tests.subsystem_tests.capacity_settlement.seed_table import seed_electricity_market, seed_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1

job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "calculation-month": CALCULATION_MONTH,
    "calculation-year": CALCULATION_YEAR,
}


class TestCapacitySettlement(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self) -> JobTestFixture:
        config = EnvironmentConfiguration()
        fixture = JobTestFixture(
            environment_configuration=config,
            job_name="CapacitySettlement",
            job_parameters=job_parameters,
        )
        seed_table(fixture)
        seed_electricity_market(fixture)
        return fixture
