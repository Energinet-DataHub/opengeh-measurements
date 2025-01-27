from dataclasses import dataclass
from uuid import UUID


@dataclass
class CapacitySettlementArgs:
    orchestration_instance_id: UUID
    calculation_month: int
    calculation_year: int
