import pyspark.sql.functions as F
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import Column

from core.bronze.domain.constants.column_names.submitted_transactions_unpacked_column_names import (
    SubmittedTransactionsUnpackedColumnNames,
)
from core.contracts.process_manager.PersistSubmittedTransaction.generated.PersistSubmittedTransaction_pb2 import Quality
from core.utility.columns_helper import invalid_if_null


def validate_orchestration_type_enum() -> Column:
    """Quality check: QCST01-01."""
    valid_orchestration_types = [name.value for name in GehCommonOrchestrationType]
    return F.col(SubmittedTransactionsUnpackedColumnNames.orchestration_type).isin(valid_orchestration_types)


def validate_quality_enum() -> Column:
    """Quality check: QCST01-02."""
    valid_qualities = [name for name in Quality.values() if name != Quality.Q_UNSPECIFIED]

    return invalid_if_null(SubmittedTransactionsUnpackedColumnNames.points).otherwise(
        F.forall(
            F.col(SubmittedTransactionsUnpackedColumnNames.Points.quality),
            lambda x: x.isNotNull() & x.isin(valid_qualities),
        )
    )


def validate_metering_point_type_enum() -> Column:
    """Quality check: QCST01-03."""
    valid_metering_point_types = [name.value for name in GehCommonMeteringPointType]
    return F.col(SubmittedTransactionsUnpackedColumnNames.metering_point_type).isin(valid_metering_point_types)


def validate_unit_enum() -> Column:
    """Quality check: QCST01-04."""
    valid_units = [name.value for name in GehCommonUnit]
    return F.col(SubmittedTransactionsUnpackedColumnNames.unit).isin(valid_units)


def validate_resolution_enum() -> Column:
    """Quality check: QCST01-05."""
    valid_resolutions = [name.value for name in GehCommonResolution]
    return F.col(SubmittedTransactionsUnpackedColumnNames.resolution).isin(valid_resolutions)
