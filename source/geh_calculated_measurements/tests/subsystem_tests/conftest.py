import pytest
from filelock import FileLock
from geh_common.data_products.electricity_market_measurements_input import (
    missing_measurements_log_metering_point_periods_v1,
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from geh_common.testing.delta_lake import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.infrastructure import MeteringPointPeriodsTable
from tests.conftest import EnvironmentConfiguration


@pytest.fixture(scope="session")
def electricity_market_data_products_created_as_tables(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory, worker_id
) -> None:
    """Create Electricity Market data products as tables.

    This enables subsystem tests to use these tables and seed them with data."""
    if worker_id == "master":
        # not executing with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        return _electricity_market_data_products_created_as_tables(spark)

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.txt"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            return
        else:
            _electricity_market_data_products_created_as_tables(spark)
            fn.write_text("done", encoding="utf-8")


def _electricity_market_data_products_created_as_tables(spark: SparkSession) -> None:
    configuration = EnvironmentConfiguration()
    # TODO BJM: create_database() and create_table() doesn't support catalog_name
    database_name = f"{configuration.catalog_name}.{configuration.electricity_market_database_name}"

    # (Re)create the database
    spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
    create_database(spark, database_name)

    # Create missing measurements log table
    create_table(
        spark,
        database_name=database_name,
        table_name=missing_measurements_log_metering_point_periods_v1.view_name,
        schema=MeteringPointPeriodsTable.schema,
    )

    # Create net consumption tables
    create_table(
        spark,
        database_name=database_name,
        table_name=net_consumption_group_6_consumption_metering_point_periods_v1.view_name,
        schema=net_consumption_group_6_consumption_metering_point_periods_v1.schema,
    )
    create_table(
        spark,
        database_name=database_name,
        table_name=net_consumption_group_6_child_metering_points_v1.view_name,
        schema=net_consumption_group_6_child_metering_points_v1.schema,
    )
