from datetime import datetime

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture

database = "electricity_market_measurements_input"
parent_table = "net_consumption_group_6_consumption_metering_point_periods_v1"
child_table = "net_consumption_group_6_child_metering_point_periods_v1"

parent_metering_point_id = "170000000000000201"
child_metering_point_id = "150000001500170200"


def _delete_seeded_data(
    job_fixture: BaseJobFixture,
) -> None:
    # PARENT
    statement = f"""
        DELETE FROM {job_fixture.environment_configuration.catalog_name}.{database}.{parent_table} 
        WHERE metering_point_id = '{parent_metering_point_id}'
    """
    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )

    # CHILD
    statement = f"""
        DELETE FROM {job_fixture.environment_configuration.catalog_name}.{database}.{child_table} 
        WHERE parent_metering_point_id = '{parent_metering_point_id}'
    """
    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )


def _seed_table(
    job_fixture: BaseJobFixture,
) -> None:
    # PARENT
    statement = f"""
    INSERT INTO {job_fixture.environment_configuration.catalog_name}.{database}.{parent_table} (
        metering_point_id,
        has_electrical_heating,
        settlement_month,
        period_from_date,
        period_to_date,
        move_in
    )
    VALUES (
        '{parent_metering_point_id}',
        {False},
        {1},
        '{datetime(2022, 12, 31, 23, 0, 0).strftime("%Y-%m-%d %H:%M:%S")}',
        '{datetime(2023, 12, 31, 23, 0, 0).strftime("%Y-%m-%d %H:%M:%S")}',
        {False}
    )
    """
    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )

    # CHILD
    statement = f"""
    INSERT INTO {job_fixture.environment_configuration.catalog_name}.{database}.{child_table} (
        metering_point_id,
        metering_type,
        parent_metering_point_id,
        coupled_date,
        uncoupled_date
    )
    VALUES (
        '{child_metering_point_id}',
        'net_consumption',
        '{parent_metering_point_id}',
        '{datetime(2022, 12, 31, 23, 0, 0)}',
        '{datetime(2023, 12, 31, 23, 0, 0)}'
    )
    """
    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )
