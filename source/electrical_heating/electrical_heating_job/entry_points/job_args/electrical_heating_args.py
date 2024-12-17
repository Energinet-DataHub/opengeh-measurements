from dataclasses import dataclass
from uuid import UUID


@dataclass
class ElectricalHeatingArgs:
    orchestration_instance_id: UUID
