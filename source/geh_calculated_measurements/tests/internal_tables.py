from dataclasses import dataclass

from geh_calculated_measurements.capacity_settlement.infrastructure import capacity_settlement_repository
from geh_calculated_measurements.common.infrastructure import calculated_measurements_repository
from geh_calculated_measurements.database_migrations import DatabaseNames
from geh_calculated_measurements.missing_measurements_log.infrastructure import missing_measurements_log_repository


@dataclass
class Table:
    database_name: str
    table_name: str


MEASUREMENTS_INTERNAL_DATABASE_NAME = DatabaseNames.MEASUREMENTS_CALCULATED_INTERNAL


class InternalTables:
    """
    Internal tables used in in the `geh_calculated_measurements` package.

    The intention of this class is to provide a single source of truth for the table names and database names used in tests.
    """

    CALCULATED_MEASUREMENTS: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name=calculated_measurements_repository.TABLE_NAME,
    )

    CAPACITY_SETTLEMENT_CALCULATIONS: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name=capacity_settlement_repository.CALCULATIONS_TABLE_NAME,
    )

    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name=capacity_settlement_repository.TEN_LARGEST_QUANTITIES_TABLE_NAME,
    )

    MISSING_MEASUREMENTS_LOG: Table = Table(
        database_name=MEASUREMENTS_INTERNAL_DATABASE_NAME,
        table_name=missing_measurements_log_repository.TABLE_NAME,
    )
