import os
import sys
import uuid
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from tests import create_job_environment_variables
from tests.capacity_settlement.job_tests import TEST_FILES_FOLDER_PATH
from tests.conftest import seed_current_measurements

_METERING_POINT_ID = "170000000000000201"
_PERIOD_START_DATETIME = datetime(2025, 1, 1, 22, 0, 0, tzinfo=timezone.utc)
_PERIOD_END_DATETIME = datetime(2025, 1, 3, 22, 0, 0, tzinfo=timezone.utc)


def test_execute(
    spark: SparkSession,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging,  # Used implicitly
    monkeypatch,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dummy_script_name",
            f"--orchestration-instance-id={orchestration_instance_id}",
            "--calculation-year=2026",
            "--calculation-month=1",
        ],
    )
    monkeypatch.setattr(os, "environ", create_job_environment_variables(str(TEST_FILES_FOLDER_PATH)))
    seed_current_measurements(
        spark=spark,
        metering_point_id=_METERING_POINT_ID,
        metering_point_type=MeteringPointType.CONSUMPTION,
        observation_time=_PERIOD_START_DATETIME,
    )

    # Act
    execute()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    actual_calculations = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_CALCULATIONS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    actual_ten_largest_quantities = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0
    assert actual_calculations.count() > 0
    assert actual_ten_largest_quantities.count() > 0
