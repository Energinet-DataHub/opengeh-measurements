import pyspark.sql.functions as F
from pyspark.sql import Column

from core.bronze.domain.constants.column_names.submitted_transactions_unpacked_column_names import (
    SubmittedTransactionsUnpackedColumnNames,
)
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType


def validate_metering_point_type_enum() -> Column:
    """Quality check: QCST01-01."""
    valid_metering_point_types = [name for name in MeteringPointType._member_names_ if name != "MPT_UNSPECIFIED"]
    return F.col(SubmittedTransactionsUnpackedColumnNames.metering_point_type).isin(valid_metering_point_types)


def validate_resolution_enum() -> Column:
    """Quality check: QCST01-02."""
    return F.lit(True)


def validate_unit_enum() -> Column:
    """Quality check: QCST01-03."""
    return F.lit(True)


def validate_quality_enum() -> Column:
    """Quality check: QCST01-04."""
    return F.lit(True)
