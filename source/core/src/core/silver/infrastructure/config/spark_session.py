from pyspark import SparkConf
from pyspark.sql.session import SparkSession


def initialize_spark() -> SparkSession:
    # Set spark config with the session timezone so that datetimes are displayed consistently (in UTC)
    max_partition_bytes_in_mb = 1000
    spark_conf = (
        SparkConf(loadDefaults=True)
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.databricks.io.cache.enabled", "True")
        .set("spark.sql.shuffle.partitions", "200")
        .set("spark.sql.files.maxPartitionBytes", str(int(max_partition_bytes_in_mb * 1024 * 1024)) + "b")
    )
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
