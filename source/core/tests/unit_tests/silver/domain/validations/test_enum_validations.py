import pytest
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import SparkSession

import core.silver.domain.validations.enum_validations as enum_validations
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import (
    MeteringPointType,
    OrchestrationType,
    Quality,
    Resolution,
    Unit,
)
from tests.helpers.builders.submitted_transactions_value_builder import PointsBuilder, SubmittedTransactionsValueBuilder

metering_point_type_enum_params = [pytest.param(x.value, 1) for x in GehCommonMeteringPointType]
orchestration_type_enum_params = [pytest.param(x.value, 1) for x in GehCommonOrchestrationType]
quality_enum_params = [pytest.param(x, 1) for x in Quality.values() if x != Quality.Q_UNSPECIFIED]
unit_enum_params = [pytest.param(x.value, 1) for x in GehCommonUnit]
resolution_enum_params = [pytest.param(x.value, 1) for x in GehCommonResolution]


@pytest.mark.parametrize(
    "orchestration_type, expected_count",
    [
        pytest.param(OrchestrationType.OT_UNSPECIFIED, 0),
        pytest.param("", 0),
        pytest.param(None, 0),
    ].__add__(orchestration_type_enum_params),
)
def test__orchestration_type_enum_validations(
    spark: SparkSession,
    orchestration_type: int,
    expected_count: int,
) -> None:
    # Arrange
    unpacked_submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark).add_row(orchestration_type=orchestration_type).build()
    )

    # Act
    actual = unpacked_submitted_transactions.filter(enum_validations.validate_orchestration_type_enum())

    # Assert
    assert actual.count() == expected_count


@pytest.mark.parametrize(
    "quality, expected_count",
    [
        pytest.param(Quality.Q_UNSPECIFIED, 0),
        pytest.param("", 0),
        pytest.param(None, 0),
    ].__add__(quality_enum_params),
)
def test__quality_enum_validations(
    spark: SparkSession,
    quality: int,
    expected_count: int,
) -> None:
    # Arrange
    points = PointsBuilder(spark).add_row(quality=quality).build()
    unpacked_submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(points=points).build()

    # Act
    actual = unpacked_submitted_transactions.filter(enum_validations.validate_quality_enum())

    # Assert
    assert actual.count() == expected_count


def test__quality_enum_validations_when_points_column_is_null__then_data_is_invalid(
    spark: SparkSession,
) -> None:
    # Arrange
    unpacked_submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(points=None).build()

    # Act
    actual = unpacked_submitted_transactions.filter(enum_validations.validate_quality_enum())

    # Assert
    assert actual.count() == 0


@pytest.mark.parametrize(
    "metering_point_type, expected_count",
    [
        pytest.param(MeteringPointType.MPT_UNSPECIFIED, 0),
        pytest.param("", 0),
        pytest.param(None, 0),
    ].__add__(metering_point_type_enum_params),
)
def test__metering_point_type_enum_validations(
    spark: SparkSession,
    metering_point_type: int,
    expected_count: int,
) -> None:
    # Arrange
    unpacked_submitted_transactions = (
        SubmittedTransactionsValueBuilder(spark).add_row(metering_point_type=metering_point_type).build()
    )

    # Act
    actual = unpacked_submitted_transactions.filter(enum_validations.validate_metering_point_type_enum())

    # Assert
    assert actual.count() == expected_count


@pytest.mark.parametrize(
    "unit, expected_count",
    [
        pytest.param(Unit.U_UNSPECIFIED, 0),
        pytest.param("", 0),
        pytest.param(None, 0),
    ].__add__(unit_enum_params),
)
def test__unit_enum_validations(
    spark: SparkSession,
    unit: int,
    expected_count: int,
) -> None:
    # Arrange
    unpacked_submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(unit=unit).build()

    # Act
    actual = unpacked_submitted_transactions.filter(enum_validations.validate_unit_enum())

    # Assert
    assert actual.count() == expected_count


@pytest.mark.parametrize(
    "resolution, expected_count",
    [
        pytest.param(Resolution.R_UNSPECIFIED, 0),
        pytest.param("", 0),
        pytest.param(None, 0),
    ].__add__(resolution_enum_params),
)
def test__resolution_enum_validations(
    spark: SparkSession,
    resolution: int,
    expected_count: int,
) -> None:
    # Arrange
    unpacked_submitted_transactions = SubmittedTransactionsValueBuilder(spark).add_row(resolution=resolution).build()

    # Act
    actual = unpacked_submitted_transactions.filter(enum_validations.validate_resolution_enum())

    # Assert
    assert actual.count() == expected_count
