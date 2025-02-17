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

    measurements = spark.createDataFrame(measurements.rdd, schema=calculated_measurements_schema, verifySchema=True)

    assert_schema(measurements.schema, calculated_measurements_schema)

    return CalculatedMeasurements(measurements)
