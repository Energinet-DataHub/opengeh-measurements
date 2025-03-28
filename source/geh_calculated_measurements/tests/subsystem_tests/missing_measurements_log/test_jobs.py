import uuid
from datetime import timedelta

import pytest

from geh_calculated_measurements.testing.utilities.job_tester import JobTester, JobTestFixture
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.missing_measurements_log.seed_table import PERIOD_START, seed_table

period_start = PERIOD_START
period_end = period_start + timedelta(days=2)
job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": period_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "period-end-datetime": period_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
}


class TestMissingMeasurementsLog(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        base_job_fixture = JobTestFixture(
            environment_configuration=config,
            job_name="MissingMeasurementsLog",
            job_parameters=job_parameters,
        )

        seed_table(base_job_fixture)

        return base_job_fixture

    @pytest.mark.skip(reason="Skipped due to issues with the telemetry data not available in the logs.")
    def test__and_then_job_telemetry_is_created(self, job_fixture) -> None:
        pass

    @pytest.mark.skip(reason="This test is temporary skipped because the storing implementation is not yet made.")
    def test__and_then_data_is_written_to_delta(self, job_fixture) -> None:
        pass
