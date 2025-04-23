import pytest
from filelock import FileLock
from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from geh_common.databricks import DatabricksApiClient
from geh_common.testing.delta_lake.delta_lake_operations import _struct_type_to_sql_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from tests.conftest import EnvironmentConfiguration


@pytest.fixture(scope="session")
def databricks_api_client() -> DatabricksApiClient:
    """Create a Databricks API client for testing.

    This enables subsystem tests to use the Databricks API client."""
    configuration = EnvironmentConfiguration()
    databricks_token = configuration.databricks_token
    databricks_host = configuration.workspace_url

    return DatabricksApiClient(databricks_token, databricks_host)


@pytest.fixture(scope="session")
def electricity_market_data_products_created_as_tables(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
    databricks_api_client: DatabricksApiClient,
    testrun_uid: str,
) -> None:
    """Create Electricity Market data products as tables.

    This enables subsystem tests to use these tables and seed them with data."""
    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / f"{testrun_uid}.txt"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            return
        else:
            _electricity_market_data_products_created_as_tables(spark, databricks_api_client)
            fn.write_text("done", encoding="utf-8")


def _electricity_market_data_products_created_as_tables(
    spark: SparkSession, databricks_api_client: DatabricksApiClient
) -> None:
    """Create Electricity Market data products as tables.

    This enables subsystem tests to use these tables and seed them with data without depending on the
    actual Electricity Market subsystem."""
    configuration = EnvironmentConfiguration()
    database_name = f"{configuration.catalog_name}.{configuration.electricity_market_database_name}"
    warehouse_id = configuration.warehouse_id

    # (Re)create the database
    databricks_api_client.execute_statement(warehouse_id, f"DROP DATABASE IF EXISTS {database_name} CASCADE")
    databricks_api_client.execute_statement(warehouse_id, f"CREATE DATABASE {database_name}")

    # Create missing measurements log table
    statement = _create_table_statement(
        database_name,
        missing_measurements_log_metering_point_periods_v1.view_name,
        missing_measurements_log_metering_point_periods_v1.schema,
    )
    databricks_api_client.execute_statement(warehouse_id, statement)

    # Create net consumption tables
    statement = _create_table_statement(
        database_name,
        net_consumption_group_6_consumption_metering_point_periods_v1.view_name,
        net_consumption_group_6_consumption_metering_point_periods_v1.schema,
    )
    databricks_api_client.execute_statement(warehouse_id, statement)

    statement = _create_table_statement(
        database_name,
        net_consumption_group_6_child_metering_points_v1.view_name,
        net_consumption_group_6_child_metering_points_v1.schema,
    )
    databricks_api_client.execute_statement(warehouse_id, statement)


def _create_table_statement(database_name: str, table_name: str, schema: StructType) -> str:
    sql_schema = _struct_type_to_sql_schema(schema)
    return f"CREATE TABLE {database_name}.{table_name} ({sql_schema}) USING DELTA"
