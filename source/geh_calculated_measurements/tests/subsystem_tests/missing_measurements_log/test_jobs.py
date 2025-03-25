import uuid
from datetime import datetime, timedelta

import pytest
from geh_common.databricks import DatabricksApiClient
from geh_common.domain.types import MeteringPointResolution
from geh_common.domain.types.quantity_quality import QuantityQuality

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

_METERING_POINT_ID = "170000060000000201"
_DEFAULT_GRID_AREA_CODE = "804"
_DEFAULT_PERIOD_START = datetime(2025, 1, 1, 23, 0, 0)
_DEFAULT_PERIOD_END = datetime(2025, 1, 2, 23, 0, 0)
_FIRST_OBSERVATION_TIME = _DEFAULT_PERIOD_START

_METERING_POINT_PERIODS_STATEMENT = f"""
        INSERT INTO electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1 (
            metering_point_id,
            grid_area_code,
            resolution,
            period_from_date,
            period_to_date,
        )
        VALUES
        "('{_METERING_POINT_ID}','{_DEFAULT_GRID_AREA_CODE}','{MeteringPointResolution.HOUR.value}','{_DEFAULT_PERIOD_START.strftime("%Y-%m-%d %H:%M:%S")}','{_DEFAULT_PERIOD_END.strftime("%Y-%m-%d %H:%M:%S")}')"
    """


job_parameters = {
    "orchestration-instance-id": uuid.uuid4(),
    "period-start-datetime": _DEFAULT_PERIOD_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "period-end-datetime": _DEFAULT_PERIOD_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
}


def _get_gold_table_rows() -> list[GoldTableRow]:
    return [
        GoldTableRow(
            metering_point_id=_METERING_POINT_ID,
            observation_time=_FIRST_OBSERVATION_TIME + timedelta(hours=i),
            quality=QuantityQuality.MEASURED.value,
        )
        for i in range(24)
    ]


def seed_test_data(environment_configuration: EnvironmentConfiguration) -> None:
    databricks_api_client = DatabricksApiClient(
        environment_configuration.databricks_token,
        environment_configuration.workspace_url,
    )
    databricks_api_client.execute_statement(
        warehouse_id=environment_configuration.warehouse_id, statement=_METERING_POINT_PERIODS_STATEMENT
    )

    # measurements
    table_seeder = GoldTableSeeder(EnvironmentConfiguration())
    gold_table_rows = _get_gold_table_rows()
    table_seeder.seed(gold_table_rows)


@pytest.fixture(scope="session")
def job_fixture(
    environment_configuration: EnvironmentConfiguration,
) -> BaseJobFixture:
    seed_test_data(environment_configuration)
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
