from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType, OrchestrationType

from geh_calculated_measurements.testing import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow


def seed_table(job_fixture: JobTestFixture) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=create_random_metering_point_id(CalculationType.CAPACITY_SETTLEMENT),
            metering_point_type=MeteringPointType.CAPACITY_SETTLEMENT,
            orchestration_type=OrchestrationType.CAPACITY_SETTLEMENT,
            observation_time=datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc),
            quantity=i,
        )
        for i in range(10)
    ]
    statement = seed_gold_table.get_statement(job_fixture.config.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)
