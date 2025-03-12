import pytest

from tests.electrical_heating.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.electrical_heating.subsystem_tests.fixtures.base_electrical_heating_fixture import BaseJobFixture
from tests.electrical_heating.subsystem_tests.job_tests.base_job_tests import BaseJobTests


class TestElectricalHeating(BaseJobTests):
    """
    Test class for electrical heating.
    """

    fixture = None

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            self.fixture = BaseJobFixture(environment_configuration, job_name="ElectricalHeating", seed_data=True)
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ) -> BaseJobFixture:
        return self.get_or_create_fixture(environment_configuration)


class TestNetConsumptionGroup6(BaseJobTests):
    """
    Test class for electrical heating net consumption for group 6.
    """

    fixture = None

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            self.fixture = BaseJobFixture(environment_configuration, job_name="NetConsumptionGroup6", seed_data=True)
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ):
        return self.get_or_create_fixture(environment_configuration)
