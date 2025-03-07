from pyspark.sql import Column
from pyspark.sql.functions import col, lit, when


def invalid_if_null(column_name: str) -> Column:
    return when(col(column_name).isNull(), lit(False))
