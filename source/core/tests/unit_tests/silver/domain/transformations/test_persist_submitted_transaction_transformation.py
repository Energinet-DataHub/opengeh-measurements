from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
import pytest
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import SparkSession

import core.silver.domain.transformations.persist_submitted_transaction_transformation as sut
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    Resolution,
    Unit,
)
from tests.helpers.builders.submitted_transactions_value_builder import (
    DecimalValue,
    Point,
    SubmittedTransactionsValueBuilder,
    Value,
)
from tests.helpers.schemas.silver_measurements_schema import silver_measurements_schema


def test__create_by_submitted_transactions__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, silver_measurements_schema, ignore_nullability=True)


def test__create_by_submitted_transaction__should_return_correct_decimal_value(spark: SparkSession) -> None:
    # Arrange
    expected_decimal_value = Decimal(1.5)
    point = Point(quantity=DecimalValue(units=1, nanos=500000000))
    submitted_transaction = SubmittedTransactionsValueBuilder(spark).add_row(points=[point]).build()

    # Act
    actual = sut.transform(submitted_transaction)

    # Assert
    assert actual.collect()[0].points[0].quantity == expected_decimal_value


def test__transform__should_align_values_to_geh_core(spark: SparkSession) -> None:
    # Arrange
    value = Value(
        orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_type=MeteringPointType.MPT_CONSUMPTION,
        unit=Unit.U_KWH,
        resolution=Resolution.R_PT1H,
    )
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(value=value).build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].orchestration_type == GehCommonOrchestrationType.SUBMITTED
    assert actual.collect()[0].metering_point_type == GehCommonMeteringPointType.CONSUMPTION
    assert actual.collect()[0].unit == GehCommonUnit.KWH
    assert actual.collect()[0].resolution == GehCommonResolution.HOUR


@pytest.mark.parametrize(
    "metering_point_type, expected_metering_point_type",
    [
        (MeteringPointType.MPT_CONSUMPTION, GehCommonMeteringPointType.CONSUMPTION),
        (MeteringPointType.MPT_CONSUMPTION_FROM_GRID, GehCommonMeteringPointType.CONSUMPTION_FROM_GRID),
        (MeteringPointType.MPT_ELECTRICAL_HEATING, GehCommonMeteringPointType.ELECTRICAL_HEATING),
        (MeteringPointType.MPT_EXCHANGE, GehCommonMeteringPointType.EXCHANGE),
        (
            MeteringPointType.MPT_EXCHANGE_REACTIVE_ENERGY,
            GehCommonMeteringPointType.EXCHANGE_REACTIVE_ENERGY,
        ),
        (MeteringPointType.MPT_INTERNAL_USE, GehCommonMeteringPointType.INTERNAL_USE),
        (MeteringPointType.MPT_NET_CONSUMPTION, GehCommonMeteringPointType.NET_CONSUMPTION),
        (MeteringPointType.MPT_NET_FROM_GRID, GehCommonMeteringPointType.NET_FROM_GRID),
        (MeteringPointType.MPT_NET_LOSS_CORRECTION, GehCommonMeteringPointType.NET_LOSS_CORRECTION),
        (MeteringPointType.MPT_NET_PRODUCTION, GehCommonMeteringPointType.NET_PRODUCTION),
        (MeteringPointType.MPT_NET_TO_GRID, GehCommonMeteringPointType.NET_TO_GRID),
        (MeteringPointType.MPT_NOT_USED, GehCommonMeteringPointType.NOT_USED),
        (MeteringPointType.MPT_OTHER_CONSUMPTION, GehCommonMeteringPointType.OTHER_CONSUMPTION),
        (MeteringPointType.MPT_OTHER_PRODUCTION, GehCommonMeteringPointType.OTHER_PRODUCTION),
        (MeteringPointType.MPT_OWN_PRODUCTION, GehCommonMeteringPointType.OWN_PRODUCTION),
        (MeteringPointType.MPT_PRODUCTION, GehCommonMeteringPointType.PRODUCTION),
        (MeteringPointType.MPT_SUPPLY_TO_GRID, GehCommonMeteringPointType.SUPPLY_TO_GRID),
        (
            MeteringPointType.MPT_SURPLUS_PRODUCTION_GROUP_6,
            GehCommonMeteringPointType.SURPLUS_PRODUCTION_GROUP_6,
        ),
        (MeteringPointType.MPT_TOTAL_CONSUMPTION, GehCommonMeteringPointType.TOTAL_CONSUMPTION),
        (MeteringPointType.MPT_VE_PRODUCTION, GehCommonMeteringPointType.VE_PRODUCTION),
        (
            MeteringPointType.MPT_WHOLESALE_SERVICES_INFORMATION,
            GehCommonMeteringPointType.WHOLESALE_SERVICES_INFORMATION,
        ),
        (MeteringPointType.MPT_ANALYSIS, GehCommonMeteringPointType.ANALYSIS),
        (MeteringPointType.MPT_CAPACITY_SETTLEMENT, GehCommonMeteringPointType.CAPACITY_SETTLEMENT),
        (
            MeteringPointType.MPT_COLLECTIVE_NET_CONSUMPTION,
            GehCommonMeteringPointType.COLLECTIVE_NET_CONSUMPTION,
        ),
        (
            MeteringPointType.MPT_COLLECTIVE_NET_PRODUCTION,
            GehCommonMeteringPointType.COLLECTIVE_NET_PRODUCTION,
        ),
    ],
)
def test__transform__should_transform_metering_point_type_to_expected(
    metering_point_type: int, expected_metering_point_type: str, spark: SparkSession
) -> None:
    # Arrange
    value = Value(
        orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_type=metering_point_type,
        unit=Unit.U_KWH,
        resolution=Resolution.R_PT1H,
    )
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(value=value).build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].metering_point_type == expected_metering_point_type


@pytest.mark.parametrize(
    "resolution, expected_resolution",
    [
        (
            Resolution.R_PT1H,
            GehCommonResolution.HOUR,
        ),
        (
            Resolution.R_PT15M,
            GehCommonResolution.QUARTER,
        ),
    ],
)
def test__transform__should_transform_resolution_to_expected(
    resolution: int, expected_resolution: str, spark: SparkSession
) -> None:
    # Arrange
    value = Value(
        orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_type=MeteringPointType.MPT_CONSUMPTION,
        unit=Unit.U_KWH,
        resolution=resolution,
    )
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(value=value).build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].resolution == expected_resolution


@pytest.mark.parametrize(
    "unit, expected_unit",
    [
        (
            Unit.U_KWH,
            GehCommonUnit.KWH,
        ),
        (
            Unit.U_KW,
            GehCommonUnit.KW,
        ),
        (
            Unit.U_MWH,
            GehCommonUnit.MWH,
        ),
        (
            Unit.U_TONNE,
            GehCommonUnit.TONNE,
        ),
        (
            Unit.U_KVARH,
            GehCommonUnit.KVARH,
        ),
    ],
)
def test__trasnform__should_transform_unit_to_expected(unit: str, expected_unit: str, spark: SparkSession) -> None:
    # Arrange
    value = Value(
        orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
        metering_point_type=MeteringPointType.MPT_CONSUMPTION,
        unit=Unit.U_KWH,
        resolution=Resolution.R_PT1H,
    )
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(value=value).build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].unit == GehCommonUnit.KWH
