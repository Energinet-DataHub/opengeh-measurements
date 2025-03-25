from enum import Enum
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, when

class MeteringPointType(Enum):
    MPT_UNSPECIFIED = "MPT_UNSPECIFIED"
    MPT_ANALYSIS = "MPT_ANALYSIS"
    MPT_COLLECTIVE_NET_CONSUMPTION = "MPT_COLLECTIVE_NET_CONSUMPTION"
    MPT_COLLECTIVE_NET_PRODUCTION = "MPT_COLLECTIVE_NET_PRODUCTION"
    MPT_CONSUMPTION = "MPT_CONSUMPTION"
    MPT_CONSUMPTION_FROM_GRID = "MPT_CONSUMPTION_FROM_GRID"
    MPT_EFFECT_PAYMENT = "MPT_EFFECT_PAYMENT"
    MPT_ELECTRICAL_HEATING = "MPT_ELECTRICAL_HEATING"
    MPT_EXCHANGE = "MPT_EXCHANGE"
    MPT_EXCHANGE_REACTIVE_ENERGY = "MPT_EXCHANGE_REACTIVE_ENERGY"
    MPT_NET_CONSUMPTION = "MPT_NET_CONSUMPTION"
    MPT_NET_FROM_GRID = "MPT_NET_FROM_GRID"
    MPT_NET_LOSS_CORRECTION = "MPT_NET_LOSS_CORRECTION"
    MPT_NET_PRODUCTION = "MPT_NET_PRODUCTION"
    MPT_NET_TO_GRID = "MPT_NET_TO_GRID"
    MPT_NOT_USED = "MPT_NOT_USED"
    MPT_OTHER_CONSUMPTION = "MPT_OTHER_CONSUMPTION"
    MPT_OTHER_PRODUCTION = "MPT_OTHER_PRODUCTION"
    MPT_OWN_PRODUCTION = "MPT_OWN_PRODUCTION"
    MPT_PRODUCTION = "MPT_PRODUCTION"
    MPT_SUPPLY_TO_GRID = "MPT_SUPPLY_TO_GRID"
    MPT_SURPLUS_PRODUCTION_GROUP_6 = "MPT_SURPLUS_PRODUCTION_GROUP_6"
    MPT_TOTAL_CONSUMPTION = "MPT_TOTAL_CONSUMPTION"
    MPT_VE_PRODUCTION = "MPT_VE_PRODUCTION"
    MPT_WHOLESALE_SERVICES_INFORMATION = "MPT_WHOLESALE_SERVICES_INFORMATION"

class MeteringPointTypeDH2(Enum):
    D01 = "D01"
    D02 = "D02"
    D03 = "D03"
    D04 = "D04"
    D05 = "D05"
    D06 = "D06"
    D07 = "D07"
    D08 = "D08"
    D10 = "D10"
    D11 = "D11"
    D12 = "D12"
    D13 = "D13"
    D14 = "D14"
    D15 = "D15"
    D17 = "D17"
    D18 = "D18"
    D20 = "D20"
    D99 = "D99"
    E17 = "E17"
    E18 = "E18"
    E20 = "E20"

def convert_dh2_mpt_to_dh3(col_name: str | Column) -> Column:
    if type(col_name) is str:
        col_name = col(col_name)

    return (
        when(col_name == MeteringPointTypeDH2.D01, lit(MeteringPointType.MPT_VE_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D02, lit(MeteringPointType.MPT_ANALYSIS))
        .when(col_name == MeteringPointTypeDH2.D03, lit(MeteringPointType.MPT_NOT_USED))
        .when(col_name == MeteringPointTypeDH2.D04, lit(MeteringPointType.MPT_SURPLUS_PRODUCTION_GROUP_6))
        .when(col_name == MeteringPointTypeDH2.D05, lit(MeteringPointType.MPT_NET_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D06, lit(MeteringPointType.MPT_SUPPLY_TO_GRID))
        .when(col_name == MeteringPointTypeDH2.D07, lit(MeteringPointType.MPT_CONSUMPTION_FROM_GRID))
        .when(col_name == MeteringPointTypeDH2.D08, lit(MeteringPointType.MPT_WHOLESALE_SERVICES_INFORMATION))
        .when(col_name == MeteringPointTypeDH2.D10, lit(MeteringPointType.MPT_NET_FROM_GRID))
        .when(col_name == MeteringPointTypeDH2.D11, lit(MeteringPointType.MPT_NET_TO_GRID))
        .when(col_name == MeteringPointTypeDH2.D12, lit(MeteringPointType.MPT_TOTAL_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D13, lit(MeteringPointType.MPT_NET_LOSS_CORRECTION))
        .when(col_name == MeteringPointTypeDH2.D14, lit(MeteringPointType.MPT_ELECTRICAL_HEATING))
        .when(col_name == MeteringPointTypeDH2.D15, lit(MeteringPointType.MPT_NET_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D17, lit(MeteringPointType.MPT_OTHER_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.D18, lit(MeteringPointType.MPT_OTHER_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.D20, lit(MeteringPointType.MPT_EXCHANGE_REACTIVE_ENERGY))
        .when(col_name == MeteringPointTypeDH2.E17, lit(MeteringPointType.MPT_CONSUMPTION))
        .when(col_name == MeteringPointTypeDH2.E18, lit(MeteringPointType.MPT_PRODUCTION))
        .when(col_name == MeteringPointTypeDH2.E20, lit(MeteringPointType.MPT_EXCHANGE))
    )
