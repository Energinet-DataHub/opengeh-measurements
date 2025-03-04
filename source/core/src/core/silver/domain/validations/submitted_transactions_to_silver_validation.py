from typing import Callable

from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame

import core.silver.domain.validations.enum_validations as enum_validations
import core.utility.rules_validator as rules_validator


def all_validations_list() -> list[Callable[[], Column]]:
    return [
        enum_validations.validate_metering_point_type_enum,
    ]


def validate(df_time_series: DataFrame) -> tuple[DataFrame, DataFrame]:
    return rules_validator.validate(
        df_time_series,
        all_validations_list(),
    )
