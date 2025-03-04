from functools import reduce
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame


def validate(df: DataFrame, validation_rules: list[Callable[[], Column]]) -> tuple[DataFrame, DataFrame]:
    for rule in validation_rules:
        df = df.withColumn(rule.__name__, rule())

    is_valid = reduce(lambda x, y: (x & F.col(y.__name__)), validation_rules, F.lit(True))

    is_valid_col_name = ValidationConstants.is_valid

    df_validated = df.withColumn(is_valid_col_name, is_valid).cache()

    df_valid = df_validated.filter(F.col(is_valid_col_name)).drop(is_valid_col_name)
    df_invalid = df_validated.filter((~F.col(is_valid_col_name)) | F.col(is_valid_col_name).isNull()).drop(
        is_valid_col_name
    )
    return df_valid, df_invalid


class ValidationConstants:
    is_valid = "is_valid"
