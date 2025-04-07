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


@pytest.mark.parametrize(
    ("database_name", "table_name", "metering_point_id"),
    [
        (
            ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME,
            ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS,
            "170000050000000201",
        ),
        (
            ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME,
            ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT,
            "150000001500170200",
        ),
        (
            ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME,
            ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT,
            "060000001500170200",
        ),
        (
            ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME,
            ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT,
            "070000001500170200",
        ),
    ],
)
def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    gold_table_seeded: None,  # Used implicitly
    electricity_market_tables_seeded: None,  # Used implicitly
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: None,  # Used implicitly
    database_name: str,
    table_name: str,
    metering_point_id: str,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))

    # Act
    execute()

    # Assert
    actual_table = spark.read.table(f"{database_name}.{table_name}").where(
        F.col(ContractColumnNames.metering_point_id) == metering_point_id
    )
    assert actual_table.count() > 0
