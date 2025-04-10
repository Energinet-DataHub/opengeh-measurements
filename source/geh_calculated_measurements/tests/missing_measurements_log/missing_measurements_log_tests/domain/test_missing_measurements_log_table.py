import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.missing_measurements_log.domain import MissingMeasurementsLogTable
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    ElectricityMarketMeasurementsInputDatabaseDefinition,
)
from tests import SPARK_CATALOG_NAME, TIME_ZONE


def test__when__debug(spark: SparkSession) -> None:
    # Arrange
    # sut = MissingMeasurementsLogTable(SPARK_CATALOG_NAME, TIME_ZONE)
    print("here------------------------------------------------")
    print(MissingMeasurementsLogTable.metering_point_id)


def test__when__(spark: SparkSession, external_dataproducts_created: None) -> None:
    # Arrange
    orchestration_instance_id = uuid.uuid4()
    print(str(orchestration_instance_id))
    sut = MissingMeasurementsLogTable(SPARK_CATALOG_NAME, TIME_ZONE, orchestration_instance_id)

    schema = StructType(
        [
            StructField(ContractColumnNames.metering_point_id, StringType(), False),
            StructField(ContractColumnNames.grid_area_code, StringType(), False),
            StructField(ContractColumnNames.resolution, StringType(), False),
            StructField(ContractColumnNames.period_from_date, TimestampType(), False),
            StructField(ContractColumnNames.period_to_date, TimestampType(), True),
        ]
    )

    data = [
        ("MP1", "GA1", "PT15M", datetime(2025, 4, 1, 22), datetime(2025, 4, 3, 22)),
    ]
    df = spark.createDataFrame(data, schema)

    table_name = f"{ElectricityMarketMeasurementsInputDatabaseDefinition.DATABASE_NAME}.{ElectricityMarketMeasurementsInputDatabaseDefinition.METERING_POINT_PERIODS}"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    # Act
    actual = sut.read()

    # Assert
    actual.show(truncate=False)
    # TODO AJW: Add assertions to check the contents of the DataFrame
    assert actual is not None
