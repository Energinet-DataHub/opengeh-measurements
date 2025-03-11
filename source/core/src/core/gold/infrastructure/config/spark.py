from pyspark import SparkConf
from pyspark.sql.session import SparkSession


def initialize_spark() -> SparkSession:
    spark_conf = SparkConf(loadDefaults=True).set("spark.sql.session.timeZone", "UTC")
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
