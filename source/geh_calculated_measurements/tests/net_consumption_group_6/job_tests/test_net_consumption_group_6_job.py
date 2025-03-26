import os
import sys
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from tests import create_job_environment_variables
from tests.net_consumption_group_6.job_tests import get_test_files_folder_path


<<<<<<< HEAD
def test_execute(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    dummy_logging: None,  # Used implicitly
) -> None:
=======
def test_execute(spark: SparkSession, monkeypatch: pytest.MonkeyPatch, dummy_logging) -> None:
>>>>>>> 807776af6e52c8ec298350107ecc1d31ca129751
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    sys_args = [
        "dummy_script_name",
        f"--orchestration-instance-id={orchestration_instance_id}",
        "--calculation-year=2026",
        "--calculation-month=1",
    ]
    monkeypatch.setattr(sys, "argv", sys_args)
    monkeypatch.setattr(os, "environ", create_job_environment_variables(get_test_files_folder_path()))

    # Act
    execute()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0
