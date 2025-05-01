import sys
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.testing import CurrentMeasurementsRow, seed_current_measurements_rows
from tests.conftest import ExternalDataProducts

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


def _seed_electricity_market(spark: SparkSession) -> None:
    capacity_settlement_metering_point_periods = ExternalDataProducts.CAPACITY_SETTLEMENT_METERING_POINT_PERIODS
    df = spark.createDataFrame(
        [
            (
                _METERING_POINT_ID,
                datetime(2020, 1, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
                900000000000000001,
                datetime(2024, 12, 31, 23, 0, 0, tzinfo=timezone.utc),
                None,
            ),
        ],
        schema=capacity_settlement_metering_point_periods.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{capacity_settlement_metering_point_periods.database_name}.{capacity_settlement_metering_point_periods.view_name}"
    )


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
    _seed_current_measurements(spark)
    _seed_electricity_market(spark)

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
