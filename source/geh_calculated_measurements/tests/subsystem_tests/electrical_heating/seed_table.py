import random
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType, OrchestrationType

from geh_calculated_measurements.testing import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow


def seed_table(job_fixture: JobTestFixture) -> None:
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=create_random_metering_point_id(CalculationType.ELECTRICAL_HEATING),
            metering_point_type=MeteringPointType.ELECTRICAL_HEATING,
            orchestration_type=OrchestrationType.ELECTRICAL_HEATING,
            observation_time=datetime(2024, 11, 30, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
        )
        for i in range(1)
    ]
    statement = seed_gold_table.get_statement(job_fixture.config.catalog_name, gold_table_rows)

    job_fixture.execute_statement(statement)
