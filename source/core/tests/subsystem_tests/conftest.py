from typing import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from tests.subsystem_tests.fixtures.kafka_fixture import KafkaFixture


@pytest.fixture
def core_fixture() -> KafkaFixture:
    return KafkaFixture()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = configure_spark_with_delta_pip(
        SparkSession.builder.config("spark.sql.streaming.schemaInference", True)
        .config("spark.default.parallelism", 1)
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        .config("spark.shuffle.spill.compress", False)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", True)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("javax.jdo.option.ConnectionUserName", "APP")
        .config("javax.jdo.option.ConnectionPassword", "mine")
        .config("datanucleus.autoCreateSchema", "true")
        .config("hive.metastore.schema.verification", "false")
        .config("hive.metastore.schema.verification.record.version", "false")
        .enableHiveSupport(),
        extra_packages=[
            "org.apache.spark:spark-protobuf_2.12:3.5.4",
            "org.apache.hadoop:hadoop-azure:3.3.2",
            "org.apache.hadoop:hadoop-common:3.3.2",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
            "io.delta:delta-spark_2.12:3.1.0",
            "io.delta:delta-core_2.12:2.3.0",
        ],
    ).getOrCreate()

    yield session

    session.stop()
