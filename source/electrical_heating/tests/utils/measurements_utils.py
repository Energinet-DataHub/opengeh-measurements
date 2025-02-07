import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType
from testcommon.dataframes import assert_schema

from opengeh_electrical_heating.infrastructure import CalculatedMeasurements
from opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.schema import (
    point,
)
from opengeh_electrical_heating.infrastructure.measurements.calculated_measurements.wrapper import (
    calculated_measurements,
)


def create_calculated_measurements_dataframe(spark: SparkSession, measurements: DataFrame) -> CalculatedMeasurements:
    measurements = measurements.withColumn(
        "points",
        F.from_json(F.col("points"), ArrayType(point)),
    )

    measurements = (
        measurements.withColumn(
            "start_datetime",
            F.to_timestamp(F.col("start_datetime"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("end_datetime", F.to_timestamp(F.col("end_datetime"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "transaction_creation_datetime",
            F.to_timestamp(F.col("transaction_creation_datetime"), "yyyy-MM-dd HH:mm:ss"),
        )
    )

    measurements = spark.createDataFrame(measurements.rdd, schema=calculated_measurements, verifySchema=True)

    assert_schema(measurements.schema, calculated_measurements)

    return CalculatedMeasurements(measurements)
