import uuid
from datetime import datetime, timedelta, timezone

import pytest
from geh_common.domain.types import MeteringPointType

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

METERING_POINT_ID = "170000040000000201"
CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)

job_parameters = {
    "orchestration-instance-id": str(uuid.uuid4()),
    "calculation-month": CALCULATION_MONTH,
    "calculation-year": CALCULATION_YEAR,
}


def _get_gold_table_rows() -> list[GoldTableRow]:
    return [
        GoldTableRow(
            metering_point_id=METERING_POINT_ID,
            observation_time=FIRST_OBSERVATION_TIME + timedelta(hours=i),
            metering_point_type=MeteringPointType.CONSUMPTION,
            quantity=i,
        )
        for i in range(10)
    ]


class TestCapacitySettlement(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        table_seeder = GoldTableSeeder(config)
        table_seeder.seed(_get_gold_table_rows())
        return JobTestFixture(
            environment_configuration=config,
            job_name="CapacitySettlement",
            job_parameters=job_parameters,
        )
