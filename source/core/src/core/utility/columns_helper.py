from pyspark.sql import Column
from pyspark.sql.functions import col, lit, when


def is_not_null(column_name: str) -> Column:
    return when(col(column_name).isNull(), lit(False))


def is_null_or_empty(column_name: str) -> Column:
    return col(column_name).isNull() | (col(column_name) == lit(""))
