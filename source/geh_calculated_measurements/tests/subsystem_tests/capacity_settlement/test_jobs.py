import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1

job_parameters = {
    "orchestration-instance-id": str(uuid.uuid4()),
    "calculation-month": CALCULATION_MONTH,
    "calculation-year": CALCULATION_YEAR,
}


class TestCapacitySettlement(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        table_seeder = GoldTableSeeder(config)
        table_seeder.seed(_get_gold_table_rows())
        return JobTestFixture(
            environment_configuration=config,
            job_name="CapacitySettlement",
            job_parameters=job_parameters,
        )
