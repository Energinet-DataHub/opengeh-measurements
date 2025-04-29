import sys
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.net_consumption_group_6.entry_point import execute
from tests import CalculationType, create_random_metering_point_id
from tests.net_consumption_group_6.job_tests.seeding import seed

parent_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
net_consumption_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
consumption_from_grid_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)
supply_to_grid_metering_point_id = create_random_metering_point_id(CalculationType.NET_CONSUMPTION)


def test_execute(
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
    execute()

    # Assert
    actual_calculated_measurements = spark.read.table(
        f"{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MEASUREMENTS_TABLE_NAME}"
    ).where(F.col(ContractColumnNames.orchestration_instance_id) == orchestration_instance_id)
    actual_calculated_measurements.show()
    assert actual_calculated_measurements.count() > 0
