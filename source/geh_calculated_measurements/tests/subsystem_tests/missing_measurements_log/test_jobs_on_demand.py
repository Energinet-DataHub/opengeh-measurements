import uuid
from datetime import datetime, timezone

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTest, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration

METERING_POINT_ID = "170000060000000201"
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)

job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": "2025-01-01T23:00:00",
    "period-end-datetime": "2025-01-02T23:00:00",
    "grid-area-codes": "[301]",
}


class TestMissingMeasurementsLogOnDemand(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        return JobTestFixture(
            environment_configuration=config,
            job_name="MissingMeasurementsLogOnDemand",
            job_parameters=job_parameters,
        )

    @pytest.mark.skip(reason="Skipped due to issues with the telemetry data not available in the logs.")
    def test__and_then_job_telemetry_is_created(self, job_fixture) -> None:
        pass

    @pytest.mark.skip(reason="This test is temporary skipped because the storing implementation is not yet made.")
    def test__and_then_data_is_written_to_delta(self, job_fixture) -> None:
        pass
