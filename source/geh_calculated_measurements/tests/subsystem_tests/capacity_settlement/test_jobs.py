import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.capacity_settlement.seed_table import seed_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1

job_parameters = {
    "orchestration-instance-id": str(uuid.uuid4()),
    "calculation-month": CALCULATION_MONTH,
    "calculation-year": CALCULATION_YEAR,
}


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    base_job_fixture = BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="CapacitySettlement",
        job_parameters=job_parameters,
    )
    seed_table(base_job_fixture)
    return base_job_fixture


class TestCapacitySettlement(BaseJobTests):
    """
    Test class for Capacity Settlement.

    """
