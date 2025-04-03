import uuid

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest
from tests.subsystem_tests.capacity_settlement.fixture import Fixture

CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1

job_parameters = {
    "orchestration-instance-id": str(uuid.uuid4()),
    "calculation-month": CALCULATION_MONTH,
    "calculation-year": CALCULATION_YEAR,
}


class TestCapacitySettlement(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        return Fixture(
            job_parameters=job_parameters,
        )
