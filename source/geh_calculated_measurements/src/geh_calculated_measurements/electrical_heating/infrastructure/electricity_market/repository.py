from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from geh_calculated_measurements.electrical_heating.domain import (
    ChildMeteringPoints,
    ConsumptionMeteringPointPeriods,
)


class Repository:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
        schema_name: str,
        consumption_points_table: str,
        child_points_table: str,
    ) -> None:
        self._spark = spark
        self._consumption_metering_point_periods = f"{catalog_name}.{schema_name}.{consumption_points_table}"
        self._child_metering_points = f"{catalog_name}.{schema_name}.{child_points_table}"

    def read_consumption_metering_point_periods(self) -> ConsumptionMeteringPointPeriods:
        return ConsumptionMeteringPointPeriods(self._spark.read.table(self._consumption_metering_point_periods))

    def read_child_metering_points(self) -> ChildMeteringPoints:
        return ChildMeteringPoints(self._spark.read.table(self._child_metering_points))


# TODO JMG: Use read_csv from opengeh_python_packages
def _read_csv(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
    sep: str = ";",
    ignored_value="[IGNORED]",
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        path (str): The path to the CSV file.
        schema (StructType): The schema of the CSV file.
        sep (str, optional): The separator of the CSV file. Defaults to ";".
        ignored_value (str, optional): Columns where all rows is equal to the
            ignored_value will be removed from the resulting DataFrame.
            Defaults to "[IGNORED]".

    Returns:
        DataFrame: The Spark DataFrame.
    """
    raw_df = spark.read.csv(path, header=True, sep=sep)

    # Check each column to see if all values are "[IGNORED]"
    ignore_check = raw_df.agg(*[F.every(F.col(c) == F.lit(ignored_value)).alias(c) for c in raw_df.columns]).collect()

    # Get the columns that should be ignored
    ignored_cols = [c for c, v in ignore_check[0].asDict().items() if v and c in schema.fieldNames()]

    raw_df = raw_df.drop(*ignored_cols)

    transforms = []
    for field in schema.fields:
        if field.name in raw_df.columns:
            if isinstance(field.dataType, T.ArrayType):
                transforms.append(F.from_json(F.col(field.name), field.dataType).alias(field.name))
            else:
                transforms.append(F.col(field.name).cast(field.dataType).alias(field.name))

    df = raw_df.select(*transforms)
    return spark.createDataFrame(df.rdd, schema=schema, verifySchema=True)
