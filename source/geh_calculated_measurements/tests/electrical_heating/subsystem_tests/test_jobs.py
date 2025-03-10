import pytest

from tests.electrical_heating.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.electrical_heating.subsystem_tests.fixtures.generic_electrical_heating_fixture import BaseJobFixture
from tests.electrical_heating.subsystem_tests.job_tests import BaseJobTests


class TestElectricalHeatingGroup6(BaseJobTests):
    """
    Test class for electrical heating net consumption for group 6.
    """

    @pytest.fixture(autouse=True)
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
        # electrical_heating_net_consumption_for_group_6_fixture,
    ) -> None:
        # self.fixture = electrical_heating_net_consumption_for_group_6_fixture
        print("HELLO")
        super().setup_fixture(
            # electrical_heating_net_consumption_for_group_6_fixture
            BaseJobFixture(
                environment_configuration, job_name="ElectricalHeatingNetConsumptionForGroup6", seed_data=False
            )
        )
