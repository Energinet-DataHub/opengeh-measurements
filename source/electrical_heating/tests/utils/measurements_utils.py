from testcommon.dataframes import assert_schema
from pyspark.sql.types import ArrayType
from electrical_heating.infrastructure.measurements_bronze.schemas.measurements_bronze_v1 import (
    measurements_bronze_v1,
    point,
)
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def create_measurements_dataframe(
    spark: SparkSession,
    measurements: DataFrame
) -> DataFrame:
    measurements = measurements.withColumn(
        "points",
        F.from_json(F.col("points"), ArrayType(point)),
    )

    measurements = (
        measurements.withColumn(
            "start_datetime",
            F.to_timestamp(F.col("start_datetime"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "end_datetime", F.to_timestamp(F.col("end_datetime"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "transaction_creation_datetime",
            F.to_timestamp(
                F.col("transaction_creation_datetime"), "yyyy-MM-dd HH:mm:ss"
            ),
        )
    )

    measurements = spark.createDataFrame(
        measurements.rdd, schema=measurements_bronze_v1, verifySchema=True
    )

    assert_schema(measurements.schema, measurements_bronze_v1)
