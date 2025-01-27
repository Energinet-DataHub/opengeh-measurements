from dataclasses import dataclass
from sqlite3.dbapi2 import Timestamp
from uuid import UUID


@dataclass
class ElectricalHeatingArgs:
    """Args for the electrical heating job."""

    orchestration_instance_id: UUID
    catalog_name: str
    time_zone: str
    period_start: Timestamp
    period_end: Timestamp
