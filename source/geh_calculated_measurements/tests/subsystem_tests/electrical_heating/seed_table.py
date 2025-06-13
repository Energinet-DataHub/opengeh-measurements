import random
from datetime import datetime, timezone

from geh_common.domain.types import (
    MeteringPointSubType,
    MeteringPointType,
    OrchestrationType,
    QuantityQuality,
)

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames
from geh_calculated_measurements.testing import JobTestFixture
from tests import CalculationType, create_random_metering_point_id
from tests.subsystem_tests import seed_gold_table
from tests.subsystem_tests.seed_gold_table import GoldTableRow

consumption_metering_point_id = create_random_metering_point_id(CalculationType.ELECTRICAL_HEATING)
child_metering_point_id = create_random_metering_point_id(CalculationType.ELECTRICAL_HEATING)

parent_table = "electrical_heating_consumption_metering_point_periods"
child_table = "electrical_heating_child_metering_points"


def _seed_gold(catalog_name):
    gold_table_rows = [
        GoldTableRow(
            metering_point_id=consumption_metering_point_id,
            metering_point_type=MeteringPointType.CONSUMPTION,
            orchestration_type=OrchestrationType.ELECTRICAL_HEATING,
            observation_time=datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
            quantity=random.uniform(0.1, 10.0),
            quality=QuantityQuality.MEASURED,
        )
    ]
    return seed_gold_table.get_statement(catalog_name, gold_table_rows)


def _electricity_market_tables_statements(catalog_name: str, database_name: str):
    statements = []
    # Consumption
    statements.append(f"""
    INSERT INTO {catalog_name}.{database_name}.{parent_table} BY NAME
    SELECT
        '{consumption_metering_point_id}' as {ContractColumnNames.metering_point_id},
        NULL as {ContractColumnNames.net_settlement_group},
        {1} as {ContractColumnNames.settlement_month},
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}' as {ContractColumnNames.period_from_date},
        NULL as {ContractColumnNames.period_to_date}
    """)

    # Child
    statements.append(f"""
    INSERT INTO {catalog_name}.{database_name}.{child_table} BY NAME
    SELECT
        '{child_metering_point_id}' as {ContractColumnNames.metering_point_id},
        '{MeteringPointType.ELECTRICAL_HEATING.value}' as {ContractColumnNames.metering_point_type},
        '{MeteringPointSubType.CALCULATED.value}' as {ContractColumnNames.metering_point_sub_type},
        '{consumption_metering_point_id}' as {ContractColumnNames.parent_metering_point_id},
        '{datetime(2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc)}' as {ContractColumnNames.coupled_date},
        NULL as {ContractColumnNames.uncoupled_date}
    """)

    return statements


def seed_table(job_fixture: JobTestFixture, orchestration_instance_id) -> None:
    catalog_name = job_fixture.config.catalog_name
    database_name = job_fixture.config.electricity_market_internal_database_name

    # Seed gold table
    job_fixture.execute_statement(_seed_gold(catalog_name))

    # Seed electricity market tables
    for statement in _electricity_market_tables_statements(catalog_name, database_name):
        job_fixture.execute_statement(statement)
