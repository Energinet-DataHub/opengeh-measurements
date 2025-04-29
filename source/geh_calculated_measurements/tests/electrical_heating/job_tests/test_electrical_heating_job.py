import sys
import uuid
from datetime import datetime
from typing import Any

from geh_common.domain.types import MeteringPointType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.entry_point import execute
from geh_calculated_measurements.testing import seed_current_measurements
from tests import create_job_environment_variables
from tests.electrical_heating.job_tests import get_test_files_folder_path
from tests.electrical_heating.job_tests.seeding import seed_electricity_market

_PARENT_METERING_POINT_ID = "170000000000000202"


def test_execute(
    spark: SparkSession,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: Any,  # Used implicitly
    monkeypatch,
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])

    seed_current_measurements(
        spark=spark,
        metering_point_id=_PARENT_METERING_POINT_ID,
        metering_point_type=MeteringPointType.CONSUMPTION,
        observation_time=datetime(2024, 1, 1, 22),
    )
    seed_electricity_market(spark)

    # Act
    execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
