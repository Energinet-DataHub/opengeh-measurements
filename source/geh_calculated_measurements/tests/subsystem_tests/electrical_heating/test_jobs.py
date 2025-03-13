import random
import uuid
from datetime import datetime
from decimal import Decimal

import pytest
from geh_common.domain.types import MeteringPointType

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

gold_table_row = GoldTableRow(
    metering_point_id="170000030000000201",
    observation_time=datetime(2024, 11, 30, 23, 0, 0),
    quantity=Decimal(random.uniform(0.1, 10.0)),  # Generate a random quantity between 0.1 and 10.0
    metering_point_type=MeteringPointType.CONSUMPTION,
)


class TestElectricalHeating(BaseJobTests):
    """
    Test class for electrical heating.
    """

    fixture = None

    params = {"orchestration-instance-id": uuid.uuid4()}

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            table_seeder = GoldTableSeeder(environment_configuration)
            table_seeder.seed(gold_table_row)

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
