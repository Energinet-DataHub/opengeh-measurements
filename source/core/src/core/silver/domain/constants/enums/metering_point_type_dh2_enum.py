from enum import Enum

from geh_common.domain.types.metering_point_type import MeteringPointType as MeteringPointTypeDH3

from pyspark.sql import Column
from pyspark.sql.functions import col, lit, when

class MeteringPointTypeDH2(Enum):
    D01 = "D01"
    D02 = "D02"
    D03 = "D03"
    D04 = "D04"
    D05 = "D05"
    D06 = "D06"
    D07 = "D07"
    D08 = "D08"
    D09 = "D09"
    D10 = "D10"
    D11 = "D11"
    D12 = "D12"
    D13 = "D13"
    D14 = "D14"
    D15 = "D15"
    D17 = "D17"
    D18 = "D18"
    D19 = "D19"
    D20 = "D20"
    D21 = "D21"
    D22 = "D22"
    D99 = "D99"
    E17 = "E17"
    E18 = "E18"
    E20 = "E20"

def convert_dh2_mpt_to_dh3(col_name: str | Column) -> Column:
    if type(col_name) is str:
        col_name = col(col_name)

    return (
        when(col_name == MeteringPointTypeDH2.E17, lit(MeteringPointTypeDH3.CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.E18, lit(MeteringPointTypeDH3.PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.E20, lit(MeteringPointTypeDH3.EXCHANGE))
        .when(col_name == MeteringPointTypeDH2.D01, lit(MeteringPointTypeDH3.VE_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D02, lit(MeteringPointTypeDH3.ANALYSIS))
        .when(col_name == MeteringPointTypeDH2.D03, lit(MeteringPointTypeDH3.NOT_USED))
        .when(col_name == MeteringPointTypeDH2.D04, lit(MeteringPointTypeDH3.SURPLUS_PRODUCTION_GROUP_6))
        .when(col_name == MeteringPointTypeDH2.D05, lit(MeteringPointTypeDH3.NET_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D06, lit(MeteringPointTypeDH3.SUPPLY_TO_GRID))
        .when(col_name == MeteringPointTypeDH2.D07, lit(MeteringPointTypeDH3.CONSUMPTION_FROM_GRID))
        .when(col_name == MeteringPointTypeDH2.D08, lit(MeteringPointTypeDH3.WHOLESALE_SERVICES_INFORMATION))
        .when(col_name == MeteringPointTypeDH2.D09, lit(MeteringPointTypeDH3.OWN_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D10, lit(MeteringPointTypeDH3.NET_FROM_GRID))
        .when(col_name == MeteringPointTypeDH2.D11, lit(MeteringPointTypeDH3.NET_TO_GRID))
        .when(col_name == MeteringPointTypeDH2.D12, lit(MeteringPointTypeDH3.TOTAL_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D13, lit(MeteringPointTypeDH3.NET_LOSS_CORRECTION))
        .when(col_name == MeteringPointTypeDH2.D14, lit(MeteringPointTypeDH3.ELECTRICAL_HEATING))
        .when(col_name == MeteringPointTypeDH2.D15, lit(MeteringPointTypeDH3.NET_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D17, lit(MeteringPointTypeDH3.OTHER_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D18, lit(MeteringPointTypeDH3.OTHER_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D19, lit(MeteringPointTypeDH3.CAPACITY_SETTLEMENT))
        .when(col_name == MeteringPointTypeDH2.D20, lit(MeteringPointTypeDH3.EXCHANGE_REACTIVE_ENERGY))
        .when(col_name == MeteringPointTypeDH2.D21, lit(MeteringPointTypeDH3.COLLECTIVE_NET_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D22, lit(MeteringPointTypeDH3.COLLECTIVE_NET_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D99, lit(MeteringPointTypeDH3.INTERNAL_USE)) # MISSING!
    )
