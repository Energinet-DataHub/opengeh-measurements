import random
import uuid
from datetime import datetime

from geh_common.domain.types import MeteringPointType, QuantityQuality

database = "measurements_gold"
table = "measurements"


def seed_table(
    job_fixture,
) -> None:
    values = ",\n".join(
        [
            f"""(
                '{170000060000000201}',
                '{"summited"}',
                '{uuid.uuid4()}',
                '{datetime(2025, 1, 1, 23, 0, 0)}',
                '{format(random.uniform(0.1, 10.0), ".3f")}',
                '{QuantityQuality.MEASURED.value}',
                '{MeteringPointType.CONSUMPTION.value}',
                "kWh",
                "PT1H",
                '{uuid.uuid4()}',
                '{"GETDATE()"}',
                '{"GETDATE()"},
                '{"GETDATE()"}',
                {False},
            )
            """
            for i in range(10)
        ]
    )

    statement = f"""
    INSERT INTO {job_fixture.environment_configuration.catalog_name}.{database}.{table} (
        metering_point_id,
        orchestration_type,
        orchestration_instance_id,
        observation_time,
        quantity,
        quality,
        metering_point_type,
        unit, 
        resolution,
        transaction_id,
        transaction_creation_datetime,
        created,
        modified,
        is_cancelled
    ) 
    VALUES {values}
    """

    job_fixture.execute_statement(statement)
