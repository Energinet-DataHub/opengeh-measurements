import pyspark.sql.functions as F
from pyspark.sql import Column

from core.bronze.domain.constants.column_names.submitted_transactions_unpacked_column_names import (
    SubmittedTransactionsUnpackedColumnNames,
)

# from core.utility.columns_helper import is_not_null
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType

# from package.enums.time_series.unit_enum import UnitEnum
# from package.enums.time_series.quality_enum import Dh2QualityEnum
# from package.enums.time_series.resolution_enum import Dh2ResolutionEnum, Dh3ResolutionEnum


# Validate Enum Values
# QC????-?? -> QC????-??


def validate_metering_point_type_enum() -> Column:
    """Quality check: QC????-??"""
    valid_metering_point_types = [name for name in MeteringPointType._member_names_ if name != "MPT_UNSPECIFIED"]
    return F.col(SubmittedTransactionsUnpackedColumnNames.metering_point_type).isin(valid_metering_point_types)


# def validate_resolution_enum() -> Column:
#     """
#     Quality check: QCTS04-03
#     """
#     values = [member.value for member in Dh2ResolutionEnum] + [member.value for member in Dh3ResolutionEnum]
#     return is_not_null(BronzeTimeSeries.time_series).otherwise(
#         F.forall(
#             F.col(BronzeTimeSeriesElement.resolution),
#             lambda x: x.isNotNull() & x.isin(values),
#         )
#     )
#
#
# def validate_unit_enum() -> Column:
#     """
#     Quality check: QCTS04-04
#     """
#     return is_not_null(BronzeTimeSeries.time_series).otherwise(
#         F.forall(
#             F.col(BronzeTimeSeriesElement.unit),
#             lambda c: c.isNotNull() & c.isin(UnitEnum._member_names_),
#         )
#     )
#
#
# def validate_quality_enum() -> Column:
#     """
#     Quality check: QCTS04-05
#     """
#     values = [member.value for member in Dh2QualityEnum]
#     return is_not_null(BronzeTimeSeries.time_series).otherwise(
#         F.forall(
#             F.col(BronzeTimeSeriesElement.values),
#             lambda x: F.forall(
#                 x,
#                 lambda c: c.quality.isNotNull() & c.quality.isin(values),
#             ),
#         )
#     )
