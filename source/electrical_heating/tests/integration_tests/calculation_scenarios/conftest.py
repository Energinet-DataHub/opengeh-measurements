from typing import Tuple

import pytest
from pyspark.sql import SparkSession, DataFrame
from testcommon.etl import read_csv

from electrical_heating.domain.calculation import _execute


@pytest.fixture(scope="module")
def test_cases(spark: SparkSession) -> list[Tuple[str, DataFrame, DataFrame]]:
    time_series_points = read_csv(spark, "time_series_points.csv")
    result = _execute(time_series_points)
