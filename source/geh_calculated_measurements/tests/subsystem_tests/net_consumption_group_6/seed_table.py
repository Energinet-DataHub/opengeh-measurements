from datetime import datetime

from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture


def _delete_parent_seeded_data(
    fully_qualified_table_name: str,
    job_fixture: BaseJobFixture,
    parent_metering_point_id,
) -> None:
    statement = f"""
        DELETE FROM {fully_qualified_table_name} 
        WHERE metering_point_id = '{parent_metering_point_id}'
    """
    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )


def _delete_child_seeded_data(
    fully_qualified_table_name: str,
    job_fixture: BaseJobFixture,
    parent_metering_point_id,
) -> None:
    statement = f"""
        DELETE FROM {fully_qualified_table_name} 
        WHERE parent_metering_point_id = '{parent_metering_point_id}'
    """
    job_fixture.databricks_api_client.execute_statement(
        warehouse_id=job_fixture.environment_configuration.warehouse_id, statement=statement
    )


def _seed_parent_table(
    fully_qualified_table_name: str,
    job_fixture: BaseJobFixture,
    parent_metering_point_id,
) -> None:
    statement = f"""
    INSERT INTO {fully_qualified_table_name} (
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


def _seed_child_table(
    fully_qualified_table_name: str,
    job_fixture: BaseJobFixture,
    parent_metering_point_id,
    child_metering_point_id,
) -> None:
    statement = f"""
    INSERT INTO {fully_qualified_table_name} (
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
