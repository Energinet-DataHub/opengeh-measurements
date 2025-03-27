import uuid
from datetime import datetime

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

METERING_POINT_ID = "170000060000000201"
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0)

job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": "2025-01-01T23:00:00",
    "period-end-datetime": "2025-01-02T23:00:00",
    "grid-area-codes": "[301]",
}


class TestMissingMeasurementsLog(JobTester):
    @property
    def fixture(self):
        config = EnvironmentConfiguration()
        return JobTestFixture(
            environment_configuration=config,
            job_name="MissingMeasurementsLogOnDemand",
            job_parameters=job_parameters,
        )
