import pyspark.sql.functions as F
from pyspark.sql import Column, SparkSession


def get_current_utc_timestamp(spark: SparkSession) -> Column:
    system_tz = spark.conf.get("spark.sql.session.timeZone")
    current_tz_time = F.current_timestamp()
    return F.to_utc_timestamp(current_tz_time, system_tz)  # type: ignore
