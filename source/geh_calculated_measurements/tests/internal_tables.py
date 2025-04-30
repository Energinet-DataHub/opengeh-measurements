from dataclasses import dataclass


@dataclass
class Table:
    database_name: str
    table_name: str


MEASUREMENTS_INTERNAL_DATABASE_NAME = "calculated_measurements_internal"


class InternalTables:
    CALCULATED_MEASUREMENTS: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name="calculated_measurements",
    )
