from typing import Callable

from pyspark.sql import Column, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from core.utility.rules_validator import validate


def test_validate(spark: SparkSession) -> None:
    # Arrange
    df = create_test_data_frame(spark)

    # Act
    (valid, invalid) = validate(df, create_validation_rules())

    # Assert
    assert valid.count() == 2
    foo_valid = valid.select(col("foo")).collect()
    bar_valid = valid.select(col("bar")).collect()
    assert foo_valid[0][0] == "A"
    assert foo_valid[1][0] == "B"
    assert bar_valid[0][0] == "ABC"
    assert bar_valid[1][0] == "ABC"

    assert invalid.count() == 3
    foo_invalid = invalid.select(col("foo")).collect()
    bar_invalid = invalid.select(col("bar")).collect()
    assert foo_invalid[0][0] == "C"
    assert foo_invalid[1][0] == "D"
    assert foo_invalid[2][0] == "E"
    assert bar_invalid[0][0] == "DEF"
    assert bar_invalid[1][0] == "ABC"
    assert bar_invalid[2][0] == "XYZ"


def create_test_data_frame(spark: SparkSession) -> DataFrame:
    data = [
        ("A", "ABC"),
        ("B", "ABC"),
        ("C", "DEF"),
        ("D", "ABC"),
        ("E", "XYZ"),
    ]

    return spark.createDataFrame(data, ["foo", "bar"])


def create_validation_rules() -> list[Callable[[], Column]]:
    return [
        validate_foo,
        validate_bar,
    ]


def validate_foo() -> Column:
    return col("foo").isin(["A", "B", "C"])


def validate_bar() -> Column:
    return col("bar") == "ABC"
