from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.application.model import (
    calculated_measurements_daily_factory,
)
from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.domain.model import CalculatedMeasurementsDaily

DEFAULT_ORCHESTRATION_INSTANCE_ID = UUID("00000000-0000-0000-0000-000000000001")
DEFAULT_ORCHESTRATION_TYPE = OrchestrationType.ELECTRICAL_HEATING
DEFAULT_METERING_POINT_TYPE = MeteringPointType.ELECTRICAL_HEATING
DEFAULT_DATE = datetime(2024, 3, 2, 23, 0, tzinfo=timezone.utc)
DEFAULT_QUANTITY = Decimal("999.123")
DEFAULT_METERING_POINT_ID = "1234567890123"
DEFAULT_TIME_ZONE = "Europe/Copenhagen"
DEFAULT_TRANSACTION_CREATION_DATETIME = datetime(2024, 3, 1, 23, 0)


def _create_daily_row(
    date: datetime = DEFAULT_DATE,
    quantity: int | Decimal = DEFAULT_QUANTITY,
    metering_point_id: str = DEFAULT_METERING_POINT_ID,
) -> Row:
    if isinstance(quantity, int):
        quantity = Decimal(quantity)

    row = {
        ContractColumnNames.metering_point_id: metering_point_id,
        ContractColumnNames.date: date,
        ContractColumnNames.quantity: quantity,
    }

    return Row(**row)


def create_daily(spark: SparkSession, data: None | Row | list[Row] = None) -> CalculatedMeasurementsDaily:
    """If data is None, a single row with default values is created."""
    if data is None:
        data = [_create_daily_row()]
    elif isinstance(data, Row):
        data = [data]

    schema = T.StructType(
        [
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), True),
            T.StructField(ContractColumnNames.date, T.TimestampType(), False),
            T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)
    return CalculatedMeasurementsDaily(df)


class TestTransactionId:
    class TestWhenNoTimeGaps:
        def test_returns_one_distinct_transaction_id(self, spark: SparkSession) -> None:
            # Arrange
            rows = [
                _create_daily_row(date=datetime(2024, 3, 1, 23, 0, tzinfo=timezone.utc)),
                _create_daily_row(date=datetime(2024, 3, 2, 23, 0, tzinfo=timezone.utc)),
                _create_daily_row(date=datetime(2024, 3, 3, 23, 0, tzinfo=timezone.utc)),
            ]
            measurements = create_daily(spark, data=rows)

            # Act
            actual = calculated_measurements_daily_factory.create(
                measurements,
                DEFAULT_ORCHESTRATION_INSTANCE_ID,
                DEFAULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
                DEFAULT_TIME_ZONE,
                DEFAULT_TRANSACTION_CREATION_DATETIME,
            )

            # Assert
            assert actual.df.select(ContractColumnNames.transaction_id).distinct().count() == 1

    class TestWhenOneTimeGap:
        def test_returns_two_distinct_transaction_id(self, spark: SparkSession) -> None:
            # Arrange
            rows = [
                _create_daily_row(date=datetime(2024, 3, 1, 23, 0, tzinfo=timezone.utc)),
                _create_daily_row(date=datetime(2024, 3, 2, 23, 0, tzinfo=timezone.utc)),
                _create_daily_row(date=datetime(2024, 3, 3, 23, 0, tzinfo=timezone.utc)),
                # Here is the gap
                _create_daily_row(date=datetime(2024, 3, 5, 23, 0, tzinfo=timezone.utc)),
                _create_daily_row(date=datetime(2024, 3, 6, 23, 0, tzinfo=timezone.utc)),
                _create_daily_row(date=datetime(2024, 3, 7, 23, 0, tzinfo=timezone.utc)),
            ]
            measurements = create_daily(spark, data=rows)

            # Act
            actual = calculated_measurements_daily_factory.create(
                measurements,
                DEFAULT_ORCHESTRATION_INSTANCE_ID,
                DEFAULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
                DEFAULT_TIME_ZONE,
                DEFAULT_TRANSACTION_CREATION_DATETIME,
            )

            # Assert
            actual_transaction_ids = (
                actual.df.orderBy(ContractColumnNames.date).select(ContractColumnNames.transaction_id).collect()
            )
            assert actual.df.select(ContractColumnNames.transaction_id).distinct().count() == 2
            assert actual_transaction_ids[0][0] == actual_transaction_ids[1][0] == actual_transaction_ids[2][0]
            assert actual_transaction_ids[3][0] == actual_transaction_ids[4][0] == actual_transaction_ids[5][0]

    class TestWhenMultipleMeteringPoints:
        def test_returns_one_transaction_id_for_each_metering_point(self, spark: SparkSession) -> None:
            # Arrange
            mp_id_1 = "1111111111111"
            mp_id_2 = "2222222222222"
            rows = [
                _create_daily_row(date=datetime(2024, 3, 1, 23, 0, tzinfo=timezone.utc), metering_point_id=mp_id_1),
                _create_daily_row(date=datetime(2024, 3, 2, 23, 0, tzinfo=timezone.utc), metering_point_id=mp_id_1),
                _create_daily_row(date=datetime(2024, 3, 1, 23, 0, tzinfo=timezone.utc), metering_point_id=mp_id_2),
                _create_daily_row(date=datetime(2024, 3, 2, 23, 0, tzinfo=timezone.utc), metering_point_id=mp_id_2),
            ]

            measurements = create_daily(spark, data=rows)

            # Act
            actual = calculated_measurements_daily_factory.create(
                measurements,
                DEFAULT_ORCHESTRATION_INSTANCE_ID,
                DEFAULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
                DEFAULT_TIME_ZONE,
                DEFAULT_TRANSACTION_CREATION_DATETIME,
            )

            # Assert
            actual_transaction_ids = actual.df.select(ContractColumnNames.transaction_id)
            assert actual_transaction_ids.distinct().count() == 2
            assert (
                actual.df.where(F.col(ContractColumnNames.metering_point_id) == mp_id_1)
                .select(F.col(ContractColumnNames.transaction_id))
                .distinct()
                .count()
                == 1
            )
            assert (
                actual.df.where(F.col(ContractColumnNames.metering_point_id) == mp_id_2)
                .select(F.col(ContractColumnNames.transaction_id))
                .distinct()
                .count()
                == 1
            )
