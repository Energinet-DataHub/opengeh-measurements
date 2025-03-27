import uuid

import pytest

from geh_calculated_measurements.missing_measurements_log.infrastructure.repository import (
    MeteringPointPeriodsDatabaseDefinition,
)
from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": "2025-01-01T23:00:00",
    "period-end-datetime": "2025-01-02T23:00:00",
}


class TestMissingMeasurementsLog(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        with pytest.MonkeyPatch.context() as m:
            m.setattr(
                MeteringPointPeriodsDatabaseDefinition,
                "METERING_POINT_PERIODS",
                "missing_measurements_log_metering_point_periods_v1",
            )
            config = EnvironmentConfiguration()
            table_seeder = GoldTableSeeder(config)
            gold_table_rows = _get_gold_table_rows()
            table_seeder.seed(gold_table_rows)
            return JobTestFixture(
                environment_configuration=config,
                job_name="MissingMeasurementsLog",
                job_parameters=job_parameters,
            )
