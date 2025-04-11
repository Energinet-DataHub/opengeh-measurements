from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
import pytest
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_quality import QuantityQuality as GehCommonQuality
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import SparkSession

import core.silver.domain.transformations.persist_submitted_transaction_transformation as sut
from core.contracts.process_manager.PersistSubmittedTransaction.generated.DecimalValue_pb2 import DecimalValue
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    Quality,
    Resolution,
    Unit,
)
from tests.helpers.builders.submitted_transactions_value_builder import (
    PointsBuilder,
    SubmittedTransactionsValueBuilder,
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
    decimal_value = DecimalValue(units=1, nanos=500000000)
    points = PointsBuilder().add_row(quantity=decimal_value).build()
    submitted_transaction = SubmittedTransactionsValueBuilder(spark).add_row(points=points).build()

    # Act
    actual = sut.transform(submitted_transaction)

    # Assert
    assert actual.collect()[0].points[0].quantity == expected_decimal_value


def test__transform__should_align_values_to_geh_core(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark)
        .add_row(
            orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
            metering_point_type=MeteringPointType.MPT_CONSUMPTION,
            unit=Unit.U_KWH,
            resolution=Resolution.R_PT1H,
        )
        .build()
    )

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
        (MeteringPointType.MPT_CONSUMPTION, GehCommonMeteringPointType.CONSUMPTION.value),
        (MeteringPointType.MPT_CONSUMPTION_FROM_GRID, GehCommonMeteringPointType.CONSUMPTION_FROM_GRID.value),
        (MeteringPointType.MPT_ELECTRICAL_HEATING, GehCommonMeteringPointType.ELECTRICAL_HEATING.value),
        (MeteringPointType.MPT_EXCHANGE, GehCommonMeteringPointType.EXCHANGE.value),
        (
            MeteringPointType.MPT_EXCHANGE_REACTIVE_ENERGY,
            GehCommonMeteringPointType.EXCHANGE_REACTIVE_ENERGY.value,
        ),
        (MeteringPointType.MPT_INTERNAL_USE, GehCommonMeteringPointType.INTERNAL_USE.value),
        (MeteringPointType.MPT_NET_CONSUMPTION, GehCommonMeteringPointType.NET_CONSUMPTION.value),
        (MeteringPointType.MPT_NET_FROM_GRID, GehCommonMeteringPointType.NET_FROM_GRID.value),
        (MeteringPointType.MPT_NET_LOSS_CORRECTION, GehCommonMeteringPointType.NET_LOSS_CORRECTION.value),
        (MeteringPointType.MPT_NET_PRODUCTION, GehCommonMeteringPointType.NET_PRODUCTION.value),
        (MeteringPointType.MPT_NET_TO_GRID, GehCommonMeteringPointType.NET_TO_GRID.value),
        (MeteringPointType.MPT_NOT_USED, GehCommonMeteringPointType.NOT_USED.value),
        (MeteringPointType.MPT_OTHER_CONSUMPTION, GehCommonMeteringPointType.OTHER_CONSUMPTION.value),
        (MeteringPointType.MPT_OTHER_PRODUCTION, GehCommonMeteringPointType.OTHER_PRODUCTION.value),
        (MeteringPointType.MPT_OWN_PRODUCTION, GehCommonMeteringPointType.OWN_PRODUCTION.value),
        (MeteringPointType.MPT_PRODUCTION, GehCommonMeteringPointType.PRODUCTION.value),
        (MeteringPointType.MPT_SUPPLY_TO_GRID, GehCommonMeteringPointType.SUPPLY_TO_GRID.value),
        (
            MeteringPointType.MPT_SURPLUS_PRODUCTION_GROUP_6,
            GehCommonMeteringPointType.SURPLUS_PRODUCTION_GROUP_6.value,
        ),
        (MeteringPointType.MPT_TOTAL_CONSUMPTION, GehCommonMeteringPointType.TOTAL_CONSUMPTION.value),
        (MeteringPointType.MPT_VE_PRODUCTION, GehCommonMeteringPointType.VE_PRODUCTION.value),
        (
            MeteringPointType.MPT_WHOLESALE_SERVICES_INFORMATION,
            GehCommonMeteringPointType.WHOLESALE_SERVICES_INFORMATION.value,
        ),
        (MeteringPointType.MPT_ANALYSIS, GehCommonMeteringPointType.ANALYSIS.value),
        (MeteringPointType.MPT_CAPACITY_SETTLEMENT, GehCommonMeteringPointType.CAPACITY_SETTLEMENT.value),
        (
            MeteringPointType.MPT_COLLECTIVE_NET_CONSUMPTION,
            GehCommonMeteringPointType.COLLECTIVE_NET_CONSUMPTION.value,
        ),
        (
            MeteringPointType.MPT_COLLECTIVE_NET_PRODUCTION,
            GehCommonMeteringPointType.COLLECTIVE_NET_PRODUCTION.value,
        ),
    ],
)
def test__transform__should_transform_metering_point_type_to_expected(
    metering_point_type: MeteringPointType, expected_metering_point_type: str, spark: SparkSession
) -> None:
    # Arrange
    submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark)
        .add_row(
            orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
            metering_point_type=metering_point_type,
            unit=Unit.U_KWH,
            resolution=Resolution.R_PT1H,
        )
        .build()
    )

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].metering_point_type == expected_metering_point_type


@pytest.mark.parametrize(
    "resolution, expected_resolution",
    [
        (
            Resolution.R_PT1H,
            GehCommonResolution.HOUR.value,
        ),
        (
            Resolution.R_PT15M,
            GehCommonResolution.QUARTER.value,
        ),
        (
            Resolution.R_P1M,
            GehCommonResolution.MONTH.value,
        ),
    ],
)
def test__transform__should_transform_resolution_to_expected(
    resolution: Resolution, expected_resolution: str, spark: SparkSession
) -> None:
    # Arrange
    submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark)
        .add_row(
            orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
            metering_point_type=MeteringPointType.MPT_CONSUMPTION,
            unit=Unit.U_KWH,
            resolution=resolution,
        )
        .build()
    )

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].resolution == expected_resolution


@pytest.mark.parametrize(
    "unit, expected_unit",
    [
        (
            Unit.U_KWH,
            GehCommonUnit.KWH.value,
        ),
        (
            Unit.U_KW,
            GehCommonUnit.KW.value,
        ),
        (
            Unit.U_MWH,
            GehCommonUnit.MWH.value,
        ),
        (
            Unit.U_TONNE,
            GehCommonUnit.TONNE.value,
        ),
        (
            Unit.U_KVARH,
            GehCommonUnit.KVARH.value,
        ),
    ],
)
def test__trasnform__should_transform_unit_to_expected(unit: Unit, expected_unit: str, spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark)
        .add_row(
            orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
            metering_point_type=MeteringPointType.MPT_CONSUMPTION,
            unit=unit,
            resolution=Resolution.R_PT1H,
        )
        .build()
    )

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].unit == expected_unit


@pytest.mark.parametrize(
    "quality, expected_quality",
    [
        (Quality.Q_CALCULATED, GehCommonQuality.CALCULATED.value),
        (Quality.Q_ESTIMATED, GehCommonQuality.ESTIMATED.value),
        (Quality.Q_MEASURED, GehCommonQuality.MEASURED.value),
    ],
)
def test__trasnform__should_transform_quality_to_expected(
    quality: Quality, expected_quality: str, spark: SparkSession
) -> None:
    # Arrange
    submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark)
        .add_row(
            orchestration_type=OrchestrationType.OT_SUBMITTED_MEASURE_DATA,
            metering_point_type=MeteringPointType.MPT_CONSUMPTION,
            resolution=Resolution.R_PT1H,
            points=PointsBuilder().add_row(quality=quality).build(),
        )
        .build()
    )

    # Act
    actual = sut.transform(submitted_transactions)

    # Assert
    assert actual.collect()[0].points[0].quality == expected_quality
