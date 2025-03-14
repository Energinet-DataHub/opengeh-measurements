import uuid
from datetime import datetime, timedelta

import pytest
from geh_common.domain.types import MeteringPointType

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

METERING_POINT_ID = "170000040000000201"
CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0)


class TestCapacitySettlement(BaseJobTests):
    """
    Test class for Capacity Settlement.

    """

    def _get_gold_table_rows(self) -> list[GoldTableRow]:
        return [
            GoldTableRow(
                metering_point_id=METERING_POINT_ID,
                observation_time=FIRST_OBSERVATION_TIME + timedelta(hours=i),
                metering_point_type=MeteringPointType.CONSUMPTION,
                quantity=i,
            )
            for i in range(10)
        ]

    fixture = None

    params = {
        "orchestration-instance-id": str(uuid.uuid4()),
        "calculation-month": CALCULATION_MONTH,
        "calculation-year": CALCULATION_YEAR,
    }

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            table_seeder = GoldTableSeeder(environment_configuration)
            table_seeder.seed(self._get_gold_table_rows())
            self.fixture = BaseJobFixture(
                environment_configuration=environment_configuration,
                job_name="CapacitySettlement",
                params=self.params,
            )
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ) -> BaseJobFixture:
        return self.get_or_create_fixture(environment_configuration)
