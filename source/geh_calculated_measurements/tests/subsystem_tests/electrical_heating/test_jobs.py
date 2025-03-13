import uuid

import pytest

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


class TestElectricalHeating(BaseJobTests):
    """
    Test class for electrical heating.
    """

    fixture = None

    params = {"orchestration-instance-id": uuid.uuid4()}

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            self.fixture = BaseJobFixture(
                environment_configuration=environment_configuration,
                job_name="ElectricalHeating",
                params=self.params,
            )
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ) -> BaseJobFixture:
        return self.get_or_create_fixture(environment_configuration)
