import datetime
from decimal import Decimal
from uuid import UUID

from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import lit

from geh_calculated_measurements.common.domain import ColumnNames
from geh_calculated_measurements.common.domain.model import calculated_measurements_factory
from geh_calculated_measurements.common.domain.model.calculated_measurements import (
    CalculatedMeasurements,
    calculated_measurements_schema,
)

DEFAULT_ORCHESTRATION_INSTANCE_ID = UUID("00000000-0000-0000-0000-000000000001")
DEFACULT_ORCHESTRATION_TYPE = OrchestrationType.ELECTRICAL_HEATING
DEFAULT_METERING_POINT_TYPE = MeteringPointType.ELECTRICAL_HEATING
DEFAULT_DATE = datetime.datetime(2024, 3, 2, 23, 0)
DEFAULT_QUANTITY = Decimal("999.123")
DEFAULT_METERING_POINT_ID = "1234567890123"


def create_row(
    date: datetime.datetime = DEFAULT_DATE,
    quantity: int | Decimal = DEFAULT_QUANTITY,
    metering_point_id: str = DEFAULT_METERING_POINT_ID,
) -> Row:
    if isinstance(quantity, int):
        quantity = Decimal(quantity)

    row = {
        ColumnNames.metering_point_id: metering_point_id,
        ColumnNames.date: date,
        ColumnNames.quantity: quantity,
    }

    return Row(**row)


def create(spark: SparkSession, data: None | Row | list[Row] = None) -> DataFrame:
    """If data is None, a single row with default values is created."""
    if data is None:
        data = [create_row()]
    elif isinstance(data, Row):
        data = [data]
    return spark.createDataFrame(data)


class TestWhenValidInput:
    def test_returns_expected_columns(self, spark: SparkSession) -> None:
        # Arrange
        df = create(spark)

        # Act
        actual = calculated_measurements_factory.create(
            df,
            DEFAULT_ORCHESTRATION_INSTANCE_ID,
            DEFACULT_ORCHESTRATION_TYPE,
            DEFAULT_METERING_POINT_TYPE,
        )

        # Assert
        assert actual.df.schema == calculated_measurements_schema


class TestWhenInputContainsIrrelevantColumn:
    def test_returns_schema_without_irrelevant_column(self, spark: SparkSession) -> None:
        # Arrange
        df = create(spark)
        irrelevant_column = "irrelevant_column"
        df = df.withColumn(irrelevant_column, lit("test"))

        # Act
        actual = CalculatedMeasurements(df)

        # Assert
        assert irrelevant_column not in actual.df.schema.fieldNames()


class TestTransactionId:
    class TestWhenNoTimeGaps:
        def test_returns_one_distinct_transaction_id(self, spark: SparkSession) -> None:
            # Arrange
            rows = [
                create_row(date=datetime.datetime(2024, 3, 1, 23, 0)),
                create_row(date=datetime.datetime(2024, 3, 2, 23, 0)),
                create_row(date=datetime.datetime(2024, 3, 3, 23, 0)),
            ]
            measurements = create(spark, data=rows)

            # Act
            actual = calculated_measurements_factory.create(
                measurements,
                DEFAULT_ORCHESTRATION_INSTANCE_ID,
                DEFACULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
            )

            # Assert
            assert actual.df.select(ColumnNames.transaction_id).distinct().count() == 1
