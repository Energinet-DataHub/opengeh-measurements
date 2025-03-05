import pytest
from pyspark.sql import SparkSession

import core.silver.domain.validations.enum_validations as enum_validations
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType
from tests.helpers.builders.submitted_transactions_builder import UnpackedSubmittedTransactionsBuilder

metering_point_type_enum_params = [pytest.param(x.value, 1) for x in MeteringPointType if x.value != "MPT_UNSPECIFIED"]
# resolution_enum_params = [pytest.param(x.value, 1) for x in Dh2ResolutionEnum] + [
#     pytest.param(x.value, 1) for x in Dh3ResolutionEnum
# ]
# unit_enum_params = [pytest.param(x.value, 1) for x in UnitEnum]
# quality_enum_params = [pytest.param(x.value, 1) for x in Dh2QualityEnum]


@pytest.mark.parametrize(
    "metering_point_type, expected_count",
    [
        pytest.param("UNKNOWN", 0),
        pytest.param("MPT_UNSPECIFIED", 0),
        pytest.param("", 0),
    ].__add__(metering_point_type_enum_params),
)
def test__metering_point_type_enum_validations(
    spark: SparkSession,
    metering_point_type: str,
    expected_count: int,
) -> None:
    unpacked_submitted_transactions = (
        UnpackedSubmittedTransactionsBuilder(spark).add_row(metering_point_type=metering_point_type).build()
    )

    actual = unpacked_submitted_transactions.filter(enum_validations.validate_metering_point_type_enum())
    assert actual.count() == expected_count
