import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

fixture = None

job_parameters = {"orchestration-instance-id": uuid.uuid4()}


@pytest.fixture(scope="session")
def setup_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    return BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="NetConsumptionGroup6",
        job_parameters=job_parameters,
    )


class TestNetConsumptionGroup6(BaseJobTests):
    """
    Test class for net consumption for group 6.
    """

    # def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
    #     if self.fixture is None:
    #         self.fixture = BaseJobFixture(
    #             environment_configuration=environment_configuration,
    #             job_name="NetConsumptionGroup6",
    #             job_parameters=self.job_parameters,
    #         )
    #     return self.fixture

    # self.get_or_create_fixture(environment_configuration)
