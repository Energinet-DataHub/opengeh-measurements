import uuid
from datetime import timedelta

import pytest

from geh_calculated_measurements.testing import JobTest, JobTestFixture
from tests.internal_tables import InternalTables
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.missing_measurements_log.seed_table import PERIOD_START, delete_seeded_data, seed_table

period_start = PERIOD_START
period_end = period_start + timedelta(days=2)
job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": period_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "period-end-datetime": period_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
}


class TestMissingMeasurementsLog(JobTest):
    @pytest.fixture(scope="class")
    def fixture(self):
        config = EnvironmentConfiguration()
        base_job_fixture = JobTestFixture(
            environment_configuration=config,
            job_name="MissingMeasurementsLog",
            job_parameters=job_parameters,
        )
        delete_seeded_data(base_job_fixture)

        seed_table(base_job_fixture)

        yield base_job_fixture

        delete_seeded_data(base_job_fixture)

        return base_job_fixture

    def test__and_then_data_is_written_to_delta(self, fixture) -> None:
        """
        Test that data is written to the delta table.
        """
        database_name = InternalTables.MISSING_MEASUREMENTS_LOG.database_name
        table_name = InternalTables.MISSING_MEASUREMENTS_LOG.table_name
        self.assert_data_written_to_delta(fixture, database_name, table_name)
