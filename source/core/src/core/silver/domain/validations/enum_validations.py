import pyspark.sql.functions as F
from pyspark.sql import Column

from core.bronze.domain.constants.column_names.submitted_transactions_unpacked_column_names import (
    SubmittedTransactionsUnpackedColumnNames,
)
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType
from core.contracts.process_manager.enums.orchestration_type import OrchestrationType
from core.contracts.process_manager.enums.quality import Quality
from core.contracts.process_manager.enums.resolution import Resolution
from core.contracts.process_manager.enums.unit import Unit
from core.utility.columns_helper import column_is_not_null


def validate_orchestration_type_enum() -> Column:
    """Quality check: QCST01-01."""
    valid_orchestration_types = [
        name for name in OrchestrationType._member_names_ if name != OrchestrationType.OT_UNSPECIFIED.value
    ]
    return F.col(SubmittedTransactionsUnpackedColumnNames.orchestration_type).isin(valid_orchestration_types)


def validate_quality_enum() -> Column:
    """Quality check: QCST01-02."""
    valid_qualities = [name for name in Quality._member_names_ if name != Quality.Q_UNSPECIFIED.value]

    return column_is_not_null(SubmittedTransactionsUnpackedColumnNames.points).otherwise(
        F.forall(
            F.col(SubmittedTransactionsUnpackedColumnNames.Points.quality),
            lambda x: x.isNotNull() & x.isin(valid_qualities),
        )
    )


def validate_metering_point_type_enum() -> Column:
    """Quality check: QCST01-03."""
    valid_metering_point_types = [
        name for name in MeteringPointType._member_names_ if name != MeteringPointType.MPT_UNSPECIFIED.value
    ]
    return F.col(SubmittedTransactionsUnpackedColumnNames.metering_point_type).isin(valid_metering_point_types)


def validate_unit_enum() -> Column:
    """Quality check: QCST01-04."""
    valid_units = [name for name in Unit._member_names_ if name != Unit.U_UNSPECIFIED.value]
    return F.col(SubmittedTransactionsUnpackedColumnNames.unit).isin(valid_units)


def validate_resolution_enum() -> Column:
    """Quality check: QCST01-05."""
    valid_resolutions = [name for name in Resolution._member_names_ if name != Resolution.R_UNSPECIFIED.value]
    return F.col(SubmittedTransactionsUnpackedColumnNames.resolution).isin(valid_resolutions)
