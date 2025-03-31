import os
import sys
import uuid
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
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
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0
