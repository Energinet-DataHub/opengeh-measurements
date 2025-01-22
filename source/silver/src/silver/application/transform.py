from pyspark.sql import DataFrame, functions as F


def transform(df: DataFrame) -> DataFrame:
    return map_bronze_to_silver(df)


def map_bronze_to_silver(df: DataFrame) -> DataFrame:
    select_list = [
        F.col("orchestration_type"),
        F.col("orchestration_instance_id"),
        F.col("metering_point_id"),
        F.col("transaction_id"),
        F.col("transaction_creation_datetime"),
        F.col("metering_point_type"),
        F.col("product"),
        F.col("unit"),
        F.col("resolution"),
        F.col("start_datetime"),
        F.col("end_datetime"),
        F.col("points"),
        F.col("created"),
    ]

    return df.select(select_list)