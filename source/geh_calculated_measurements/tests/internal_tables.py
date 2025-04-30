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

    CAPACITY_SETTLEMENT_CALCULATIONS: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name="capacity_settlement_calculations",
    )

    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name="capacity_settlement_ten_largest_quantities",
    )

    MISSING_MEASUREMENTS_LOG: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name="missing_measurements_log",
    )
