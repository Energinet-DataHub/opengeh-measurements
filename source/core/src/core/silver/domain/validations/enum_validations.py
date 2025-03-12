import pyspark.sql.functions as F
from pyspark.sql import Column

from core.bronze.domain.constants.column_names.submitted_transactions_unpacked_column_names import (
    SubmittedTransactionsUnpackedColumnNames,
)

from core.contracts.process_manager.enums.quality import Quality
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.
from core.utility.columns_helper import invalid_if_null


def validate_orchestration_type_enum() -> Column:
    """Quality check: QCST01-01."""
    valid_orchestration_types = [
        name for name in GehCommonOrchestrationType._member_names_
    ]
    return F.col(SubmittedTransactionsUnpackedColumnNames.orchestration_type).isin(valid_orchestration_types)


def validate_quality_enum() -> Column:
    """Quality check: QCST01-02."""
    valid_qualities = [name for name in Quality._member_names_ if name != Quality.Q_UNSPECIFIED.value]

    return invalid_if_null(SubmittedTransactionsUnpackedColumnNames.points).otherwise(
        F.forall(
            F.col(SubmittedTransactionsUnpackedColumnNames.Points.quality),
            lambda x: x.isNotNull() & x.isin(valid_qualities),
        )
    )


def validate_metering_point_type_enum() -> Column:
    """Quality check: QCST01-03."""
    valid_metering_point_types = [
        name for name in GehCommonMeteringPointType._member_names_
    ]
    return F.col(SubmittedTransactionsUnpackedColumnNames.metering_point_type).isin(valid_metering_point_types)


def validate_unit_enum() -> Column:
    """Quality check: QCST01-04."""
    valid_units = [name for name in GehCommonUnit._member_names_]
    return F.col(SubmittedTransactionsUnpackedColumnNames.unit).isin(valid_units)


def validate_resolution_enum() -> Column:
    """Quality check: QCST01-05."""
    valid_resolutions = [name for name in GehCommonResolution._member_names_]
    return F.col(SubmittedTransactionsUnpackedColumnNames.resolution).isin(valid_resolutions)
