from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pyspark.sql.types as T
import pytest
from geh_common.domain.types import MeteringPointType, OrchestrationType
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames, calculated_measurements_factory

DEFAULT_ORCHESTRATION_INSTANCE_ID = UUID("00000000-0000-0000-0000-000000000001")
DEFAULT_ORCHESTRATION_TYPE = OrchestrationType.ELECTRICAL_HEATING
DEFAULT_METERING_POINT_TYPE = MeteringPointType.ELECTRICAL_HEATING
DEFAULT_DATE = datetime(2024, 3, 2, 23, 0)
DEFAULT_QUANTITY = Decimal("999.123")
DEFAULT_METERING_POINT_ID = "1234567890123"
DEFAULT_TIME_ZONE = "Europe/Copenhagen"
DEFAULT_TRANSACTION_CREATION_DATETIME = datetime(2024, 3, 1, 23, 0)


def create_row(
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


def create(spark: SparkSession, data: None | Row | list[Row] = None) -> DataFrame:
    """If data is None, a single row with default values is created."""
    if data is None:
        data = [create_row()]
    elif isinstance(data, Row):
        data = [data]

    schema = T.StructType(
        [
            T.StructField(ContractColumnNames.metering_point_id, T.StringType(), True),
            T.StructField(ContractColumnNames.date, T.TimestampType(), False),
            T.StructField(ContractColumnNames.quantity, T.DecimalType(18, 3), False),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


class TestWhenValidInput:
    def test_returns_expected_columns(self, spark: SparkSession) -> None:
        # Arrange
        expected_columns = [
            ContractColumnNames.metering_point_id,
            ContractColumnNames.observation_time,
            ContractColumnNames.quantity,
            ContractColumnNames.orchestration_instance_id,
            ContractColumnNames.orchestration_type,
            ContractColumnNames.metering_point_type,
            ContractColumnNames.transaction_creation_datetime,
            ContractColumnNames.transaction_id,
        ]
        df = create(spark)

        # Act
        actual = calculated_measurements_factory.deprecated_create(
            df,
            DEFAULT_ORCHESTRATION_INSTANCE_ID,
            DEFAULT_ORCHESTRATION_TYPE,
            DEFAULT_METERING_POINT_TYPE,
            DEFAULT_TIME_ZONE,
            DEFAULT_TRANSACTION_CREATION_DATETIME,
        )

        # Assert
        assert set(actual.df.columns) == set(expected_columns)


class TestWhenInputContainsIrrelevantColumn:
    def test_returns_schema_without_irrelevant_column(self, spark: SparkSession) -> None:
        # Arrange
        df = create(spark)
        irrelevant_column = "irrelevant_column"
        df = df.withColumn(irrelevant_column, F.lit("test"))

        # Act
        actual = calculated_measurements_factory.deprecated_create(
            df,
            DEFAULT_ORCHESTRATION_INSTANCE_ID,
            DEFAULT_ORCHESTRATION_TYPE,
            DEFAULT_METERING_POINT_TYPE,
            DEFAULT_TIME_ZONE,
            DEFAULT_TRANSACTION_CREATION_DATETIME,
        )

        # Assert
        assert irrelevant_column not in actual.df.columns


class TestTransactionId:
    class TestWhenNoTimeGaps:
        def test_returns_one_distinct_transaction_id(self, spark: SparkSession) -> None:
            # Arrange
            rows = [
                create_row(date=datetime(2024, 3, 1, 23, 0)),
                create_row(date=datetime(2024, 3, 2, 23, 0)),
                create_row(date=datetime(2024, 3, 3, 23, 0)),
            ]
            measurements = create(spark, data=rows)

            # Act
            actual = calculated_measurements_factory.deprecated_create(
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
                create_row(date=datetime(2024, 3, 1, 23, 0)),
                create_row(date=datetime(2024, 3, 2, 23, 0)),
                create_row(date=datetime(2024, 3, 3, 23, 0)),
                # Here is the gap
                create_row(date=datetime(2024, 3, 5, 23, 0)),
                create_row(date=datetime(2024, 3, 6, 23, 0)),
                create_row(date=datetime(2024, 3, 7, 23, 0)),
            ]
            measurements = create(spark, data=rows)

            # Act
            actual = calculated_measurements_factory.deprecated_create(
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

    class TestWhenPeriodCrossesDaylightSavingTime:
        @pytest.mark.parametrize(
            "dates",
            [
                (  # Entering DST
                    [
                        datetime(2024, 3, 30, 23),
                        datetime(2024, 3, 31, 22),
                        datetime(2024, 4, 1, 22),
                    ]
                ),
                (  # Exiting DST
                    [
                        datetime(2024, 10, 26, 22),
                        datetime(2024, 10, 27, 23),
                        datetime(2024, 10, 28, 23),
                    ]
                ),
            ],
        )
        def test_returns_one_transaction_id(self, spark: SparkSession, dates: list[datetime]) -> None:
            # Arrange
            rows = [create_row(date=date) for date in dates]
            measurements = create(spark, data=rows)

            # Act
            actual = calculated_measurements_factory.deprecated_create(
                measurements,
                DEFAULT_ORCHESTRATION_INSTANCE_ID,
                DEFAULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
                DEFAULT_TIME_ZONE,
                DEFAULT_TRANSACTION_CREATION_DATETIME,
            )

            # Assert
            assert actual.df.select(ContractColumnNames.transaction_id).distinct().count() == 1

    class TestWhenMultipleMeteringPoints:
        def test_returns_one_transaction_id_for_each_metering_point(self, spark: SparkSession) -> None:
            # Arrange
            mp_id_1 = "1111111111111"
            mp_id_2 = "2222222222222"
            rows = [
                create_row(date=datetime(2024, 3, 1, 23, 0), metering_point_id=mp_id_1),
                create_row(date=datetime(2024, 3, 2, 23, 0), metering_point_id=mp_id_1),
                create_row(date=datetime(2024, 3, 1, 23, 0), metering_point_id=mp_id_2),
                create_row(date=datetime(2024, 3, 2, 23, 0), metering_point_id=mp_id_2),
            ]

            measurements = create(spark, data=rows)

            # Act
            actual = calculated_measurements_factory.deprecated_create(
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

    class TestWhenMultipleOrchestrationInstanceIdsWithSameData:
        def test_returns_different_transaction_ids(self, spark: SparkSession) -> None:
            # Arrange
            measurements = create(spark)
            orchestration_instance_id_1 = UUID("00000000-0000-0000-0000-000000000001")
            orchestration_instance_id_2 = UUID("00000000-0000-0000-0000-000000000002")

            # Act
            actual_1 = calculated_measurements_factory.deprecated_create(
                measurements,
                orchestration_instance_id_1,
                DEFAULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
                DEFAULT_TIME_ZONE,
                DEFAULT_TRANSACTION_CREATION_DATETIME,
            )
            actual_2 = calculated_measurements_factory.deprecated_create(
                measurements,
                orchestration_instance_id_2,
                DEFAULT_ORCHESTRATION_TYPE,
                DEFAULT_METERING_POINT_TYPE,
                DEFAULT_TIME_ZONE,
                DEFAULT_TRANSACTION_CREATION_DATETIME,
            )

            # Assert
            assert actual_1.df.select(ContractColumnNames.transaction_id).distinct().count() == 1
            assert actual_2.df.select(ContractColumnNames.transaction_id).distinct().count() == 1
            assert (
                actual_1.df.collect()[0][ContractColumnNames.transaction_id]
                != actual_2.df.collect()[0][ContractColumnNames.transaction_id]
            )
