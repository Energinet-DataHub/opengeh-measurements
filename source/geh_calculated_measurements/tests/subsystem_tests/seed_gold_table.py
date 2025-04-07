import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType
from geh_common.domain.types.quantity_quality import QuantityQuality

from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)

database = MeasurementsGoldDatabaseDefinition.DATABASE_NAME
table = MeasurementsGoldDatabaseDefinition.MEASUREMENTS_TABLE_NAME


@dataclass
class GoldTableRow:
    metering_point_id: str
    orchestration_type: str = "submitted"
    orchestration_instance_id: uuid.UUID = uuid.uuid4()
    observation_time: datetime = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)
    quantity: float = 1.7
    quality: str = QuantityQuality.MEASURED.value
    metering_point_type: MeteringPointType = MeteringPointType.CONSUMPTION
    transaction_id: uuid.UUID = uuid.uuid4()


def get_statement(catalog_name: str, rows: list[GoldTableRow]) -> str:
    values = ",\n".join(
        [
            f"""
            (
                '{row.metering_point_id}',
                '{row.orchestration_type}',
                '{str(row.orchestration_instance_id)}',
                '{row.observation_time.strftime("%Y-%m-%d %H:%M:%S")}',
                '{format(row.quantity, ".3f")}',
                '{row.quality}',
                '{row.metering_point_type.value}',
                'kWh',
                'PT1H',
                '{str(row.transaction_id)}',
                GETDATE(),
                false,
                GETDATE(),
                GETDATE()
            )
            """
            for row in rows
        ]
    )
    return f"""
            INSERT INTO {catalog_name}.{database}.{table} (
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
                is_cancelled,
                created,
                modified
            )
            VALUES {values}
            """
