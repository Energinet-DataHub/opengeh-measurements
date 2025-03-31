import os
import sys
import uuid
from typing import Any

import pytest
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from geh_calculated_measurements.net_consumption_group_6.infrastucture.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from tests import create_job_environment_variables
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    dummy_logging: None,  # Used implicitly
    electricity_market_calculated_measurements_create_and_seed_tables: Any,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))

    # Act
    execute()

    # Assert
    parent_calculated_measurements = spark.read.table(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS}"
    )
    assert parent_calculated_measurements.count() > 0

    child_calculated_measurements = spark.read.table(
        f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT}"
    )
    assert child_calculated_measurements.count() > 0
