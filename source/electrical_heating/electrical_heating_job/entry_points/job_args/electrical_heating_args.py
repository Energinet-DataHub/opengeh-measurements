from dataclasses import dataclass
from uuid import UUID


@dataclass
class ElectricalHeatingArgs:
    """
    Args for the electrical heating job.
    """

    orchestration_instance_id: UUID  # or str
    created_by_user_id: UUID  # or str
