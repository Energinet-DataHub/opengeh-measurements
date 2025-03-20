from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import SparkSession

import core.silver.domain.transformations.measurements_transformation as sut
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
from tests.silver.schemas.silver_measurements_schema import silver_measurements_schema


def test__create_by_submitted_transactions__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row().build()

    # Act
    actual = sut.transform(spark, submitted_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, silver_measurements_schema, ignore_nullability=True)


def test__create_by_submitted_transaction__should_return_correct_decimal_value(spark: SparkSession) -> None:
    # Arrange
    expected_decimal_value = Decimal(1.5)
    point = Point(quantity=DecimalValue(units=1, nanos=500000000))
    submitted_transaction = SubmittedTransactionsValueBuilder(spark).add_row(points=[point]).build()

    # Act
    actual = sut.transform(spark, submitted_transaction)

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
    actual = sut.transform(spark, submitted_transactions)

    # Assert
    assert actual.collect()[0].orchestration_type == GehCommonOrchestrationType.SUBMITTED.value
    assert actual.collect()[0].metering_point_type == GehCommonMeteringPointType.CONSUMPTION.value
    assert actual.collect()[0].unit == GehCommonUnit.KWH.value
    assert actual.collect()[0].resolution == GehCommonResolution.HOUR.value
