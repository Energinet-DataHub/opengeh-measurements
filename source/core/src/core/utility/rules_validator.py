from functools import reduce
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame


def validate(df: DataFrame, validation_rules: list[Callable[[], Column]]) -> tuple[DataFrame, DataFrame]:
    validation_columns = {rule.__name__: rule() for rule in validation_rules}

    df = df.withColumns(validation_columns)

    is_valid = reduce(lambda x, y: (x & F.col(y.__name__)), validation_rules, F.lit(True))

    df_validated = df.withColumn(ValidationConstants.is_valid, is_valid).cache()

    df_valid = drop_validation_columns(validation_rules, df_validated)

    df_invalid = df_validated.filter(
        (~F.col(ValidationConstants.is_valid)) | F.col(ValidationConstants.is_valid).isNull()
    ).drop(ValidationConstants.is_valid)

    return df_valid, df_invalid


def drop_validation_columns(validation_rules, df_validated) -> DataFrame:
    df_valid = df_validated.filter(F.col(ValidationConstants.is_valid)).drop(ValidationConstants.is_valid)

    for rule in validation_rules:
        df_valid = df_valid.drop(rule.__name__)

    return df_valid


class ValidationConstants:
    is_valid = "is_valid"
