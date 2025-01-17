import pytest
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    TimestampType,
    DecimalType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from testcommon.dataframes import (
    assert_dataframes_and_schemas,
    AssertDataframesConfiguration,
)
from testcommon.etl import get_then_names, TestCases


@pytest.mark.parametrize("name", get_then_names())
def test_case(
    spark,
    name: str,
    test_cases: TestCases,
    assert_dataframes_configuration: AssertDataframesConfiguration,
) -> None:
    schema = StructType(
        [
            StructField("metering_point_id", StringType(), True),
            StructField("date", TimestampType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("comment", StringType(), True),
        ]
    )

    raw_df = spark.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm'Z'").csv(
        "tmp_measurements.csv", header=True, sep=";", schema=schema
    )
    raw_df.printSchema()

    # raw_df = spark.read.csv("tmp_measurements.csv", header=True, sep=";")
    # raw_df = raw_df.withColumn("date", col("date").cast("timestamp"))
    raw_df.show()
    # # raw_df.show()
    # schema = StructType([StructField("date", StringType(), True)])
    #
    # # Create a DataFrame with a single row
    # data = [("2026-01-08T03:00Z",)]
    # # data = [("2025-01-09T15:30+02:00",)]
    # df = spark.createDataFrame(data, schema)
    # # df = df.withColumn("date", col("date").cast("timestamp"))
    # df = df.select(to_timestamp(df.date, "yyyy-MM-dd'T'HH:mm'Z'").alias("date"))
    # df.show()

    # test_case = test_cases[name]
    # test_case.expected.show()
    # test_case.actual.show()
    # assert_dataframes_and_schemas(
    #     actual=test_case.actual,
    #     expected=test_case.expected,
    #     configuration=assert_dataframes_configuration,
    # )
