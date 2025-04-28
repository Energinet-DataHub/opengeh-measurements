import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointResolution, MeteringPointType, OrchestrationType, QuantityUnit
from geh_common.domain.types.quantity_quality import QuantityQuality

from tests import MEASUREMENTS_GOLD_TABLE_NAME

database = "measurements_gold"
table = MEASUREMENTS_GOLD_TABLE_NAME


@dataclass
class GoldTableRow:
    metering_point_id: str
    metering_point_type: MeteringPointType
    orchestration_type: OrchestrationType
    orchestration_instance_id: uuid.UUID = uuid.uuid4()
    observation_time: datetime = datetime(2025, 1, 1, 23, 0, 0, tzinfo=timezone.utc)
    quantity: float = 1.7
    quality: str = QuantityQuality.MEASURED.value
    transaction_id: uuid.UUID = uuid.uuid4()


def get_statement(catalog_name: str, rows: list[GoldTableRow]) -> str:
    values = ",\n".join(
        [
            f"""
            (
                '{row.metering_point_id}',
                '{row.metering_point_type.value}',
                '{row.orchestration_type}',
                '{str(row.orchestration_instance_id)}',
                '{row.observation_time.strftime("%Y-%m-%d %H:%M:%S")}',
                '{format(row.quantity, ".3f")}',
                '{row.quality}',
                '{QuantityUnit.KWH.value}',
                '{MeteringPointResolution.HOUR.value}',
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
                metering_point_type,
                orchestration_type,
                orchestration_instance_id,
                observation_time,
                quantity,
                quality,
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
