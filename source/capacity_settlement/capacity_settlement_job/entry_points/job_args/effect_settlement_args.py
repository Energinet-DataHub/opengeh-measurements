from dataclasses import dataclass
from uuid import UUID


@dataclass
class CapacitySettlementArgs:
    orchestration_instance_id: UUID
    actor_id: str
