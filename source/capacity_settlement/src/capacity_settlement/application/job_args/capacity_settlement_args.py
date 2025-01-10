from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass
class CapacitySettlementArgs:
    orchestration_instance_id: UUID
    calculation_period_start: datetime
    calculation_period_end: datetime
