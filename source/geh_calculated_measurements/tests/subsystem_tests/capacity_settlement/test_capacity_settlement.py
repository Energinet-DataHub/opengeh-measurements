import unittest
import uuid
from datetime import datetime, timedelta

import pytest
from azure.monitor.query import LogsQueryStatus
from databricks.sdk.service.jobs import RunResultState
from geh_common.domain.types import MeteringPointType

from tests.subsystem_tests.capacity_settlement.fixtures.capacity_settlement_fixture import (
    CapacitySettlementFixture,
)
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

METERING_POINT_ID = "170000040000000201"
CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0)


class TestCapacitySettlement(unittest.TestCase):
    """
    Subsystem test that verifies a Databricks capacity settlement job runs successfully to completion.
    """

    def _get_gold_table_rows(self) -> list[GoldTableRow]:
        return [
            GoldTableRow(
                metering_point_id=METERING_POINT_ID,
                observation_time=FIRST_OBSERVATION_TIME + timedelta(hours=i),
                metering_point_type=MeteringPointType.CONSUMPTION,
            )
            for i in range(10)
        ]

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(self) -> None:
        environment_configuration = EnvironmentConfiguration()
        self.fixture = CapacitySettlementFixture()
        table_seeder = GoldTableSeeder(environment_configuration)
        table_seeder.seed(self._get_gold_table_rows())

    @pytest.mark.order(1)
    def test__given_job_input(self) -> None:
        # Act
        self.fixture.job_state.calculation_input.job_id = self.fixture.get_job_id()
        self.fixture.job_state.calculation_input.orchestration_instance_id = uuid.uuid4()
        self.fixture.job_state.calculation_input.year = CALCULATION_YEAR
        self.fixture.job_state.calculation_input.month = CALCULATION_MONTH

        # Assert
        assert self.fixture.job_state.calculation_input.job_id is not None

    @pytest.mark.order(2)
    def test__when_job_is_started(self) -> None:
        # Act
        self.fixture.job_state.run_id = self.fixture.start_job(self.fixture.job_state.calculation_input)

        # Assert
        assert self.fixture.job_state.run_id is not None

    @pytest.mark.order(3)
    def test__then_job_is_completed(self) -> None:
        # Act
        self.fixture.job_state.run_result_state = self.fixture.wait_for_job_to_completion(self.fixture.job_state.run_id)

        # Assert
        assert self.fixture.job_state.run_result_state == RunResultState.SUCCESS, (
            f"The Job {self.fixture.job_state.calculation_input.job_id} did not complete successfully: {self.fixture.job_state.run_result_state.value}"
        )

    @pytest.mark.order(4)
    def test__and_then_job_telemetry_is_created(self) -> None:
        # Arrange
        if self.fixture.job_state.run_result_state != RunResultState.SUCCESS:
            raise Exception("A previous test did not complete successfully.")

        query = f"""
        AppTraces
        | where Properties["Subsystem"] == 'measurements'
        | where Properties["orchestration_instance_id"] == '{self.fixture.job_state.calculation_input.orchestration_instance_id}'
        """

        # Act
        actual = self.fixture.wait_for_log_query_completion(query)

        # Assert
        assert actual.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully: {actual.status}"
