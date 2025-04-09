import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.capacity_settlement.seed_table import seed_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1

job_parameters = {
    "orchestration-instance-id": str(uuid.uuid4()),
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
        return fixture
