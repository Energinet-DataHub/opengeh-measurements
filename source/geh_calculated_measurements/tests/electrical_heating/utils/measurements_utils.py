from ast import alias
import pyspark.sql.functions as F
from geh_common.testing.dataframes import assert_schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType

from geh_calculated_measurements.electrical_heating.infrastructure import (
    CalculatedMeasurements,
)
from geh_calculated_measurements.electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
    point,
)


def create_calculated_measurements_dataframe(spark: SparkSession, measurements: DataFrame) -> CalculatedMeasurements:
    measurements = measurements.select(
        "*"
        F.from_json(F.col("points"), ArrayType(point)).alias("points"),
        F.to_timestamp(F.col("start_datetime"), "yyyy-MM-dd HH:mm:ss").alias("start_datetime"),
        F.to_timestamp(F.col("end_datetime"), "yyyy-MM-dd HH:mm:ss").alias("end_datetime"),
        F.to_timestamp(F.col("transaction_creation_datetime"), "yyyy-MM-dd HH:mm:ss").alias("transaction_creation_datetime"),
    )

    measurements = spark.createDataFrame(measurements.rdd, schema=calculated_measurements_schema, verifySchema=True)

    assert_schema(measurements.schema, calculated_measurements_schema)

    return CalculatedMeasurements(measurements)
