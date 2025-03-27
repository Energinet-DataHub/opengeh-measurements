import uuid
from datetime import datetime, timezone

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

job_parameters = {"orchestration-instance-id": uuid.uuid4()}

gold_table_row = GoldTableRow(
    metering_point_id="170000030000000201",
    observation_time=datetime(2024, 11, 30, 23, 0, 0, tzinfo=timezone.utc),
    quantity=random.uniform(0.1, 10.0),
    metering_point_type=MeteringPointType.CONSUMPTION,
)


class TestElectricalHeating(JobTester):
    """
    Test class for electrical heating.
    """

    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        table_seeder = GoldTableSeeder(config)
        table_seeder.seed(gold_table_row)
        return JobTestFixture(
            environment_configuration=config,
            job_name="ElectricalHeating",
            job_parameters=job_parameters,
        )
