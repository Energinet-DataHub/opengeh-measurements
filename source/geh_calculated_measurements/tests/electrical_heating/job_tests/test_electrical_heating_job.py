import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from geh_common.domain.types import MeteringPointSubType, MeteringPointType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
)
from geh_calculated_measurements.electrical_heating.entry_point import execute
from geh_calculated_measurements.testing import seed_current_measurements
from tests.conftest import ExternalDataProducts

_PARENT_METERING_POINT_ID = "170000000000000202"
_CHILD_METERING_POINT_ID = "140000000000170202"
_PERIOD_FROM = datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc)


def _seed_electricity_market(spark: SparkSession) -> None:
    # Consumption
    consumption_metering_point_periods = ExternalDataProducts.ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS
    df = spark.createDataFrame(
        [
            (_PARENT_METERING_POINT_ID, 2, 1, _PERIOD_FROM, None),
        ],
        schema=consumption_metering_point_periods.schema,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{consumption_metering_point_periods.database_name}.{consumption_metering_point_periods.view_name}"
    )

    # Child
    child_metering_points = ExternalDataProducts.ELECTRICAL_HEATING_CHILD_METERING_POINTS
    df = spark.createDataFrame(
        [
            (
                _CHILD_METERING_POINT_ID,
                MeteringPointType.ELECTRICAL_HEATING.value,
                MeteringPointSubType.CALCULATED.value,
                _PARENT_METERING_POINT_ID,
                _PERIOD_FROM,
                None,
            ),
        ],
        schema=child_metering_points.schema,
    )

    df.write.format("delta").mode("append").saveAsTable(
        f"{child_metering_points.database_name}.{child_metering_points.view_name}"
    )


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
        observation_time=_PERIOD_FROM,
    )
    _seed_electricity_market(spark)

    # Act
    execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col("orchestration_instance_id") == orchestration_instance_id)
    assert actual.count() > 0
