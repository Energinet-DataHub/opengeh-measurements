import os
import uuid
from datetime import datetime, timezone

from geh_common.domain.types import MeteringPointResolution, MeteringPointType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.missing_measurements_log.entry_point import execute
from tests import CalculationType, create_job_environment_variables, create_random_metering_point_id
from tests.conftest import seed_current_measurements
from tests.external_data_products import ExternalDataProducts

_METERING_POINT_ID = create_random_metering_point_id(CalculationType.MISSING_MEASUREMENTS_LOG)
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
    seed_current_measurements(
        spark=spark,
        metering_point_id=_METERING_POINT_ID,
        metering_point_type=MeteringPointType.CONSUMPTION,
        observation_time=_PERIOD_START_DATETIME,
    )
    _seed_metering_point_periods(spark)

    # Act
    execute()

    # Assert
    actual = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MISSING_MEASUREMENTS_LOG_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == str(orchestration_instance_id))
    assert actual.count() > 0
