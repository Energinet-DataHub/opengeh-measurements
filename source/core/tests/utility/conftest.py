from typing import Generator

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark_conf = SparkConf(loadDefaults=True).set("spark.sql.session.timeZone", "UTC")
    session = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    yield session

    session.stop()
