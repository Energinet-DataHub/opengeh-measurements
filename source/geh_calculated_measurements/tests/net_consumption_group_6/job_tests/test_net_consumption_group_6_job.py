import os
import sys
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from tests import create_job_environment_variables
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path

DATABASE = ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME
PARENT_TABLE = (
    ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS
)
CHILD_TABLE = ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT


def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    gold_table_seeded: None,  # Used implicitly
    electricity_market_tables_seeded: None,  # Used implicitly
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))

    # Act
    execute()

    # Assert
    actual_parent_table = spark.read.table(f"{DATABASE}.{PARENT_TABLE}").where(
        F.col(ContractColumnNames.metering_point_id) == "170000050000000201"
    )
    assert actual_parent_table.count() > 0

    actual_child_table_net_consumption = spark.read.table(f"{DATABASE}.{CHILD_TABLE}").where(
        F.col(ContractColumnNames.metering_point_id) == "150000001500170200"
    )
    assert actual_child_table_net_consumption.count() > 0
    actual_child_table_supply_to_grid = spark.read.table(f"{DATABASE}.{CHILD_TABLE}").where(
        F.col(ContractColumnNames.metering_point_id) == "060000001500170200"
    )
    assert actual_child_table_supply_to_grid.count() > 0
    actual_child_table_consumption_from_grid = spark.read.table(f"{DATABASE}.{CHILD_TABLE}").where(
        F.col(ContractColumnNames.metering_point_id) == "070000001500170200"
    )
    assert actual_child_table_consumption_from_grid.count() > 0
