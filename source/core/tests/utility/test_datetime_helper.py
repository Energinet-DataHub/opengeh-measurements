import datetime

from pyspark.sql import SparkSession

import core.utility.datetime_helper as sut  # Replace with the actual module name


def test__get_current_utc_timestamp__should_return_current_utc_timestamp(spark: SparkSession) -> None:
    # Act
    actual_column = sut.get_current_utc_timestamp(spark)

    # Convert the Column to a single-row DataFrame and collect the value
    actual_time = spark.createDataFrame([(1,)], ["id"]).select(actual_column.alias("ts")).collect()[0]["ts"]
    expected_time = datetime.datetime.now()

    # Assert: Allow up to 60-seconds difference
    assert abs((actual_time - expected_time).total_seconds()) < 60
