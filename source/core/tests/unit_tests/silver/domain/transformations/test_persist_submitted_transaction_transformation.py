from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
import pytest
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import SparkSession

import core.silver.domain.transformations.persist_submitted_transaction_transformation as sut
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType as CoreMeteringPointType
from core.contracts.process_manager.enums.orchestration_type import OrchestrationType as CoreOrchestrationType
from core.contracts.process_manager.enums.resolution import Resolution as CoreResolution
from core.contracts.process_manager.enums.unit import Unit as CoreUnit
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
        orchestration_type=CoreOrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_type=CoreMeteringPointType.MPT_CONSUMPTION.value,
        unit=CoreUnit.U_KWH.value,
        resolution=CoreResolution.R_PT1H.value,
    )
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(value=value).build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].orchestration_type == GehCommonOrchestrationType.SUBMITTED.value
    assert actual.collect()[0].metering_point_type == GehCommonMeteringPointType.CONSUMPTION.value
    assert actual.collect()[0].unit == GehCommonUnit.KWH.value
    assert actual.collect()[0].resolution == GehCommonResolution.HOUR.value


@pytest.mark.parametrize(
    "metering_point_type, expected_metering_point_type",
    [
        (CoreMeteringPointType.MPT_CONSUMPTION.value, GehCommonMeteringPointType.CONSUMPTION.value),
        (CoreMeteringPointType.MPT_CONSUMPTION_FROM_GRID.value, GehCommonMeteringPointType.CONSUMPTION_FROM_GRID.value),
        (CoreMeteringPointType.MPT_ELECTRICAL_HEATING.value, GehCommonMeteringPointType.ELECTRICAL_HEATING.value),
        (CoreMeteringPointType.MPT_EXCHANGE.value, GehCommonMeteringPointType.EXCHANGE.value),
        (
            CoreMeteringPointType.MPT_EXCHANGE_REACTIVE_ENERGY.value,
            GehCommonMeteringPointType.EXCHANGE_REACTIVE_ENERGY.value,
        ),
        (CoreMeteringPointType.MPT_INTERNAL_USE.value, GehCommonMeteringPointType.INTERNAL_USE.value),
        (CoreMeteringPointType.MPT_NET_CONSUMPTION.value, GehCommonMeteringPointType.NET_CONSUMPTION.value),
        (CoreMeteringPointType.MPT_NET_FROM_GRID.value, GehCommonMeteringPointType.NET_FROM_GRID.value),
        (CoreMeteringPointType.MPT_NET_LOSS_CORRECTION.value, GehCommonMeteringPointType.NET_LOSS_CORRECTION.value),
        (CoreMeteringPointType.MPT_NET_PRODUCTION.value, GehCommonMeteringPointType.NET_PRODUCTION.value),
        (CoreMeteringPointType.MPT_NET_TO_GRID.value, GehCommonMeteringPointType.NET_TO_GRID.value),
        (CoreMeteringPointType.MPT_NOT_USED.value, GehCommonMeteringPointType.NOT_USED.value),
        (CoreMeteringPointType.MPT_OTHER_CONSUMPTION.value, GehCommonMeteringPointType.OTHER_CONSUMPTION.value),
        (CoreMeteringPointType.MPT_OTHER_PRODUCTION.value, GehCommonMeteringPointType.OTHER_PRODUCTION.value),
        (CoreMeteringPointType.MPT_OWN_PRODUCTION.value, GehCommonMeteringPointType.OWN_PRODUCTION.value),
        (CoreMeteringPointType.MPT_PRODUCTION.value, GehCommonMeteringPointType.PRODUCTION.value),
        (CoreMeteringPointType.MPT_SUPPLY_TO_GRID.value, GehCommonMeteringPointType.SUPPLY_TO_GRID.value),
        (
            CoreMeteringPointType.MPT_SURPLUS_PRODUCTION_GROUP_6.value,
            GehCommonMeteringPointType.SURPLUS_PRODUCTION_GROUP_6.value,
        ),
        (CoreMeteringPointType.MPT_TOTAL_CONSUMPTION.value, GehCommonMeteringPointType.TOTAL_CONSUMPTION.value),
        (CoreMeteringPointType.MPT_VE_PRODUCTION.value, GehCommonMeteringPointType.VE_PRODUCTION.value),
        (
            CoreMeteringPointType.MPT_WHOLESALE_SERVICES_INFORMATION.value,
            GehCommonMeteringPointType.WHOLESALE_SERVICES_INFORMATION.value,
        ),
        (CoreMeteringPointType.MPT_ANALYSIS.value, GehCommonMeteringPointType.ANALYSIS.value),
        (CoreMeteringPointType.MPT_CAPACITY_SETTLEMENT.value, GehCommonMeteringPointType.CAPACITY_SETTLEMENT.value),
        (
            CoreMeteringPointType.MPT_COLLECTIVE_NET_CONSUMPTION.value,
            GehCommonMeteringPointType.COLLECTIVE_NET_CONSUMPTION.value,
        ),
        (
            CoreMeteringPointType.MPT_COLLECTIVE_NET_PRODUCTION.value,
            GehCommonMeteringPointType.COLLECTIVE_NET_PRODUCTION.value,
        ),
    ],
)
def test__transform__should_transform_metering_point_type_to_expected(
    metering_point_type: str, expected_metering_point_type: str, spark: SparkSession
) -> None:
    # Arrange
    value = Value(
        orchestration_type=CoreOrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_type=metering_point_type,
        unit=CoreUnit.U_KWH.value,
        resolution=CoreResolution.R_PT1H.value,
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
            CoreResolution.R_PT1H.value,
            GehCommonResolution.HOUR.value,
        ),
        (
            CoreResolution.R_PT15M.value,
            GehCommonResolution.QUARTER.value,
        ),
        (
            CoreResolution.R_P1M.value,
            GehCommonResolution.MONTH.value,
        ),
    ],
)
def test__transform__should_transform_resolution_to_expected(
    resolution: str, expected_resolution: str, spark: SparkSession
) -> None:
    # Arrange
    value = Value(
        orchestration_type=CoreOrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_type=CoreMeteringPointType.MPT_CONSUMPTION.value,
        unit=CoreUnit.U_KWH.value,
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
            CoreUnit.U_KWH.value,
            GehCommonUnit.KWH.value,
        ),
        (
            CoreUnit.U_KW.value,
            GehCommonUnit.KW.value,
        ),
        (
            CoreUnit.U_MWH.value,
            GehCommonUnit.MWH.value,
        ),
        (
            CoreUnit.U_TONNE.value,
            GehCommonUnit.TONNE.value,
        ),
        (
            CoreUnit.U_KVARH.value,
            GehCommonUnit.KVARH.value,
        ),
    ],
)
def test__trasnform__should_transform_unit_to_expected(unit: str, expected_unit: str, spark: SparkSession) -> None:
    # Arrange
    value = Value(
        orchestration_type=CoreOrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        metering_point_type=CoreMeteringPointType.MPT_CONSUMPTION.value,
        unit=CoreUnit.U_KWH.value,
        resolution=CoreResolution.R_PT1H.value,
    )
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(value=value).build()

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].unit == GehCommonUnit.KWH.value
