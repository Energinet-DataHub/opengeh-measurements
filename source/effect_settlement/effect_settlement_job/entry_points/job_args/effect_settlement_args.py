from dataclasses import dataclass
from uuid import UUID


@dataclass
class EffectSettlementArgs:
    orchestration_instance_id: UUID
    actor_id: str
