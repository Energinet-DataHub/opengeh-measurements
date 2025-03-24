import datetime
import uuid
from datetime import timedelta

import pytest
from geh_common.domain.types.quantity_quality import QuantityQuality

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

METERING_POINT_ID = "170000060000000201"
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0)

job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": "2025-01-01T23:00:00",
    "period-end-datetime": "2025-01-02T23:00:00",
}


def _get_gold_table_rows() -> list[GoldTableRow]:
    return [
        GoldTableRow(
            metering_point_id=METERING_POINT_ID,
            observation_time=FIRST_OBSERVATION_TIME + timedelta(hours=i),
            quality=QuantityQuality.MEASURED.value,
        )
        for i in range(24)
    ]


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    table_seeder = GoldTableSeeder(environment_configuration)
    gold_table_rows = _get_gold_table_rows()
    table_seeder.seed(gold_table_rows)
    return BaseJobFixture(
        environment_configuration=environment_configuration,
        job_name="MissingMeasurementsLog",
        job_parameters=job_parameters,
    )


class TestMissingMeasurementsLog(BaseJobTests):
    """
    Test class for missing measurements log.
    """

    @pytest.mark.skip(reason="Skipped due to issues with the telemetry data not available in the logs.")
    def test__and_then_job_telemetry_is_created(self, job_fixture: BaseJobFixture) -> None:
        pass

    @pytest.mark.skip(reason="This test is temporary skipped because the storing implementation is not yet made.")
    def test__and_then_data_is_written_to_delta(
        self, environment_configuration: EnvironmentConfiguration, job_fixture: BaseJobFixture
    ) -> None:
        pass
