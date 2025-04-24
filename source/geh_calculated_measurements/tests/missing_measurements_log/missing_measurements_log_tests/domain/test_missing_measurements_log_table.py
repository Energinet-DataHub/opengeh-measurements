from geh_common.testing.dataframes import assert_schema
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.missing_measurements_log.domain import MissingMeasurementsLogTable


def test__schema_is_correct() -> None:
    # Arrange
    expected = StructType(
        [
            StructField(ContractColumnNames.orchestration_instance_id, StringType(), False),
            StructField(ContractColumnNames.metering_point_id, StringType(), False),
            StructField(ContractColumnNames.date, TimestampType(), False),
        ]
    )

    # Act
    actual = MissingMeasurementsLogTable.schema

    # Assert
    assert_schema(actual, expected)
