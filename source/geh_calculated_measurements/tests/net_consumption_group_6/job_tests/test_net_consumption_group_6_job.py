import sys
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute_cenc_daily, execute_cnc_daily
from tests import CalculationType, create_random_metering_point_id
from tests.internal_tables import InternalTables
from tests.net_consumption_group_6.job_tests.seeding import seed

parent_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
net_consumption_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
consumption_from_grid_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
supply_to_grid_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)


def test_execute_cenc_daily(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])

    seed(
        spark,
        parent_metering_point_id,
        net_consumption_metering_point_id,
        consumption_from_grid_metering_point_id,
        supply_to_grid_metering_point_id,
    )

    # Act
    execute_cenc_daily()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{InternalTables.CALCULATED_MEASUREMENTS.database_name}.{InternalTables.CALCULATED_MEASUREMENTS.table_name}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0


def test_execute_cnc_daily(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    migrations_executed: None,  # Used implicitly
    external_dataproducts_created: None,  # Used implicitly
    dummy_logging: None,  # Used implicitly
) -> None:
    # Arrange
    orchestration_instance_id = str(uuid.uuid4())
    monkeypatch.setattr(sys, "argv", ["dummy_script_name", "--orchestration-instance-id", orchestration_instance_id])

    seed(
        spark,
        parent_metering_point_id,
        consumption_from_grid_metering_point_id,
        net_consumption_metering_point_id,
        supply_to_grid_metering_point_id,
    )

    # Act
    execute_cnc_daily()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{InternalTables.CALCULATED_MEASUREMENTS.database_name}.{InternalTables.CALCULATED_MEASUREMENTS.table_name}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    assert actual_calculated_measurements.count() > 0
