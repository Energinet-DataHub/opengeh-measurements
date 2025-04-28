from datetime import datetime, timezone
from decimal import Decimal
import os
import uuid

from pyspark.sql import SparkSession, functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.missing_measurements_log.entry_point import execute
from tests import create_job_environment_variables
from tests.external_data_products import ExternalDataProducts
from geh_common.domain.types import MeteringPointResolution, MeteringPointType, QuantityQuality

_METERING_POINT_ID = "170000000000000201"
_PERIOD_START_DATETIME = datetime(2025, 1, 1, 22, 0, 0, tzinfo=timezone.utc)
_PERIOD_END_DATETIME = datetime(2025, 1, 3, 22, 0, 0, tzinfo=timezone.utc)


def _create_job_arguments(orchestration_instance_id: uuid.UUID) -> list[str]:
    return [
        "dummy_script_name",
        "--orchestration-instance-id",
        str(orchestration_instance_id),
        "--period-start-datetime",
        _PERIOD_START_DATETIME.strftime("%Y-%m-%d %H:%M:%S"),
        "--period-end-datetime",
        _PERIOD_END_DATETIME.strftime("%Y-%m-%d %H:%M:%S"),
    ]


def _seed_current_measurements(spark: SparkSession) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema

    observation_time = _PERIOD_START_DATETIME

    measurements = spark.createDataFrame(
        [
            (
                _METERING_POINT_ID,
                observation_time,
                Decimal("1.0"),
                QuantityQuality.MEASURED.value,
                MeteringPointType.CONSUMPTION.value,
            )
        ],
        schema=schema,
    )

    measurements.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )


def _seed_metering_point_periods(spark: SparkSession) -> None:
    database_name = ExternalDataProducts.MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS.database_name
    table_name = ExternalDataProducts.MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS.view_name
    schema = ExternalDataProducts.MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS.schema

    df = spark.createDataFrame(
        [
            (
                _METERING_POINT_ID,
                "804",
                MeteringPointResolution.HOUR.value,
                _PERIOD_START_DATETIME,
                _PERIOD_END_DATETIME,
            )
        ],
        schema=schema,
    )

    df.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="append",
    )


def test_execute(
    spark: SparkSession,
    monkeypatch,
    dummy_logging: None,  # Used implicitly
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = uuid.uuid4()
    monkeypatch.setattr("sys.argv", _create_job_arguments(orchestration_instance_id))
    monkeypatch.setattr(os, "environ", create_job_environment_variables())
    _seed_current_measurements(spark)
    _seed_metering_point_periods(spark)

    # Act
    execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MISSING_MEASUREMENTS_LOG_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == str(orchestration_instance_id))
    assert actual.count() > 0
