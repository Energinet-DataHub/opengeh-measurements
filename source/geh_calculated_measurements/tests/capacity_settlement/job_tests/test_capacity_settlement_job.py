import os
import sys
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.testing import CurrentMeasurementsRow, seed_current_measurements_rows
from tests import create_job_environment_variables
from tests.capacity_settlement.job_tests import TEST_FILES_FOLDER_PATH
from tests.internal_tables import InternalTables

_METERING_POINT_ID = "170000000000000201"
_PERIOD_START = datetime(2026, 1, 1, 22, 0, 0)


def _seed_current_measurements(spark: SparkSession) -> None:
    """We need at least 10 measurements to get some output"""
    rows = [
        CurrentMeasurementsRow(
            metering_point_id=_METERING_POINT_ID, observation_time=_PERIOD_START + timedelta(hours=i)
        )
        for i in range(10)
    ]
    seed_current_measurements_rows(spark, rows)


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
    _seed_current_measurements(spark)

    # Act
    execute()

    # Assert
    for table in [
        InternalTables.CALCULATED_MEASUREMENTS,
        InternalTables.CAPACITY_SETTLEMENT_CALCULATIONS,
        InternalTables.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES,
    ]:
        actual = spark.read.table(f"{table.database_name}.{table.table_name}").where(
            F.col("orchestration_instance_id") == orchestration_instance_id
        )
        assert actual.count() > 0
