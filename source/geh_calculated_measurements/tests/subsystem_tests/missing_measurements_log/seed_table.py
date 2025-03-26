import random
import uuid
from datetime import datetime, timedelta

from geh_common.domain.types import MeteringPointType
from geh_common.domain.types.quantity_quality import QuantityQuality

from tests.subsystem_tests.base_resources.base_job_tests import BaseJobFixture

database = "measurements_gold"
table = "measurements"


def seed_table(
    job_fixture: BaseJobFixture,
) -> None:
    values = ",\n".join(
        [
            f"""(
                '{170000060000000201}',
                'summited',
                '{uuid.uuid4()}',
                '{datetime(2025, 1, 1, 23, 0, 0) + timedelta(hours=i)}',
                '{format(random.uniform(0.1, 10.0), ".3f")}',
                '{QuantityQuality.MEASURED.value}',
                '{MeteringPointType.CONSUMPTION.value}',
                'kWh',
                'PT1H',
                '{uuid.uuid4()}',
                GETDATE(), 
                GETDATE(),
                GETDATE(),
                {False}
            )
            """
            for i in range(24)
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

    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )
