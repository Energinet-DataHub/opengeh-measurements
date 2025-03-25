import uuid
from datetime import datetime, timedelta

import pytest
from geh_common.domain.types.quantity_quality import QuantityQuality

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.missing_measurements_log.seed_metering_point_periods import (
    MeteringPointPeriodsRow,
    MeteringPointPeriodsTableSeeder,
)
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

METERING_POINT_ID = "170000060000000201"
DEFAULT_PERIOD_START = datetime(2025, 1, 1, 23, 0, 0)
DEFAULT_PERIOD_END = datetime(2025, 1, 2, 23, 0, 0)
FIRST_OBSERVATION_TIME = DEFAULT_PERIOD_START
LAST_OBSERVATION_TIME = FIRST_OBSERVATION_TIME - timedelta(hours=23)

job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": DEFAULT_PERIOD_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "period-end-datetime": DEFAULT_PERIOD_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
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


def seed_test_data() -> None:
    # measurements
    table_seeder = GoldTableSeeder(EnvironmentConfiguration())
    gold_table_rows = _get_gold_table_rows()
    table_seeder.seed(gold_table_rows)

    # metering point periods
    metering_point_periods_row = MeteringPointPeriodsRow(
        metering_point_id=METERING_POINT_ID,
        grid_area_code="804",
        period_from_date=DEFAULT_PERIOD_START,
        period_to_date=DEFAULT_PERIOD_END,
    )
    metering_point_periods_table_seeder = MeteringPointPeriodsTableSeeder(EnvironmentConfiguration())
    metering_point_periods_table_seeder.seed(metering_point_periods_row)


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    seed_test_data()
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
