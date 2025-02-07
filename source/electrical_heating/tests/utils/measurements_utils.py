import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from testcommon.dataframes import assert_schema

from opengeh_electrical_heating.infrastructure import CalculatedMeasurements
from opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    calculated_measurements_schema,
)


def create_calculated_measurements_dataframe(spark: SparkSession, measurements: DataFrame) -> CalculatedMeasurements:
    measurements = measurements.withColumn(
        "transaction_creation_datetime",
        F.to_timestamp(F.col("transaction_creation_datetime"), "yyyy-MM-dd HH:mm:ss"),
    )

    measurements = spark.createDataFrame(measurements.rdd, schema=calculated_measurements_schema, verifySchema=True)

    assert_schema(measurements.schema, calculated_measurements_schema)

    return CalculatedMeasurements(measurements)
