from enum import Enum

from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, when


# Lifted from opengeh-migrations.
class UnitEnumDH2(Enum):
    KWH = "KWH"
    MWH = "MWH"
    KVARH = "KVARH"
    KW = "KW"
    T = "T"


def convert_dh2_unit_to_dh3(col_name: str | Column) -> Column:
    if type(col_name) is str:
        col_name = col(col_name)

    return (
        when(col_name == UnitEnumDH2.KWH.value, lit(GehCommonUnit.KWH.value))
        .when(col_name == UnitEnumDH2.MWH.value, lit(GehCommonUnit.MWH.value))
        .when(col_name == UnitEnumDH2.KVARH.value, lit(GehCommonUnit.KVARH.value))
        .when(col_name == UnitEnumDH2.KW.value, lit(GehCommonUnit.KW.value))
        .when(col_name == UnitEnumDH2.T.value, lit(GehCommonUnit.TONNE.value))
        .otherwise(col_name)
    )
