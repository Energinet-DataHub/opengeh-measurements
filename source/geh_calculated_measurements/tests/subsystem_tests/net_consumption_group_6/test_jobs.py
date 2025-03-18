import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class TestNetConsumptionGroup6(BaseJobTests):
    """
    Test class for net consumption for group 6.
    """

    fixture = None

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            job_parameters = {"orchestration-instance-id": uuid.uuid4()}
            self.fixture = BaseJobFixture(
                environment_configuration=environment_configuration,
                job_name="NetConsumptionGroup6",
                job_parameters=job_parameters,
            )
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ) -> BaseJobFixture:
        return self.get_or_create_fixture(environment_configuration)
