import sys
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.capacity_settlement.entry_point import execute
from geh_calculated_measurements.testing import CurrentMeasurementsRow, seed_current_measurements_rows
from tests.conftest import ExternalDataProducts
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


def _seed_electricity_market(spark: SparkSession) -> None:
    capacity_settlement_metering_point_periods = InternalTables.CAPACITY_SETTLEMENT_CALCULATIONS
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
        schema=ExternalDataProducts.CAPACITY_SETTLEMENT_METERING_POINT_PERIODS.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{capacity_settlement_metering_point_periods.database_name}.{capacity_settlement_metering_point_periods.table_name}"
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
    for table in [
        InternalTables.CALCULATED_MEASUREMENTS,
        InternalTables.CAPACITY_SETTLEMENT_CALCULATIONS,
        InternalTables.CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES,
    ]:
        actual = spark.read.table(f"{table.database_name}.{table.table_name}").where(
            F.col("orchestration_instance_id") == orchestration_instance_id
        )
        assert actual.count() > 0
