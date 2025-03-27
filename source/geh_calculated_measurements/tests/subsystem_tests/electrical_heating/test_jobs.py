import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.electrical_heating.seed_table import seed_table
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    base_job_fixture = BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="ElectricalHeating",
        job_parameters=job_parameters,
    )

    # Insert seeded data
    seed_table(base_job_fixture)

    return base_job_fixture


class TestElectricalHeating(BaseJobTests):
    """
    Test class for electrical heating.
    """
