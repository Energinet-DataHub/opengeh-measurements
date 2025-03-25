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
        when(col_name == MeteringPointTypeDH2.E17.value, lit(MeteringPointTypeDH3.CONSUMPTION.value))
        .when(col_name == MeteringPointTypeDH2.E18.value, lit(MeteringPointTypeDH3.PRODUCTION.value))
        .when(col_name == MeteringPointTypeDH2.E20.value, lit(MeteringPointTypeDH3.EXCHANGE.value))
        .when(col_name == MeteringPointTypeDH2.D01.value, lit(MeteringPointTypeDH3.VE_PRODUCTION.value))
        .when(col_name == MeteringPointTypeDH2.D02.value, lit(MeteringPointTypeDH3.ANALYSIS.value))
        .when(col_name == MeteringPointTypeDH2.D03.value, lit(MeteringPointTypeDH3.NOT_USED.value))
        .when(col_name == MeteringPointTypeDH2.D04.value, lit(MeteringPointTypeDH3.SURPLUS_PRODUCTION_GROUP_6.value))
        .when(col_name == MeteringPointTypeDH2.D05.value, lit(MeteringPointTypeDH3.NET_PRODUCTION.value))
        .when(col_name == MeteringPointTypeDH2.D06.value, lit(MeteringPointTypeDH3.SUPPLY_TO_GRID.value))
        .when(col_name == MeteringPointTypeDH2.D07.value, lit(MeteringPointTypeDH3.CONSUMPTION_FROM_GRID.value))
        .when(
            col_name == MeteringPointTypeDH2.D08.value, lit(MeteringPointTypeDH3.WHOLESALE_SERVICES_INFORMATION.value)
        )
        .when(col_name == MeteringPointTypeDH2.D09.value, lit(MeteringPointTypeDH3.OWN_PRODUCTION.value))
        .when(col_name == MeteringPointTypeDH2.D10.value, lit(MeteringPointTypeDH3.NET_FROM_GRID.value))
        .when(col_name == MeteringPointTypeDH2.D11.value, lit(MeteringPointTypeDH3.NET_TO_GRID.value))
        .when(col_name == MeteringPointTypeDH2.D12.value, lit(MeteringPointTypeDH3.TOTAL_CONSUMPTION.value))
        .when(col_name == MeteringPointTypeDH2.D13.value, lit(MeteringPointTypeDH3.NET_LOSS_CORRECTION.value))
        .when(col_name == MeteringPointTypeDH2.D14.value, lit(MeteringPointTypeDH3.ELECTRICAL_HEATING.value))
        .when(col_name == MeteringPointTypeDH2.D15.value, lit(MeteringPointTypeDH3.NET_CONSUMPTION.value))
        .when(col_name == MeteringPointTypeDH2.D17.value, lit(MeteringPointTypeDH3.OTHER_CONSUMPTION.value))
        .when(col_name == MeteringPointTypeDH2.D18.value, lit(MeteringPointTypeDH3.OTHER_CONSUMPTION.value))
        .when(col_name == MeteringPointTypeDH2.D19.value, lit(MeteringPointTypeDH3.CAPACITY_SETTLEMENT.value))
        .when(col_name == MeteringPointTypeDH2.D20.value, lit(MeteringPointTypeDH3.EXCHANGE_REACTIVE_ENERGY.value))
        .when(col_name == MeteringPointTypeDH2.D21.value, lit(MeteringPointTypeDH3.COLLECTIVE_NET_PRODUCTION.value))
        .when(col_name == MeteringPointTypeDH2.D22.value, lit(MeteringPointTypeDH3.COLLECTIVE_NET_PRODUCTION.value))
        # .when(col_name == MeteringPointTypeDH2.D99, lit(MeteringPointTypeDH3.INTERNAL_USE)) # MISSING!
        .otherwise(col_name)
    )
