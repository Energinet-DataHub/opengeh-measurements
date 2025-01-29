import datetime
from uuid import UUID

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opengeh_electrical_heating.infrastructure.electricity_market.data_values.resolution import Resolution
from opengeh_electrical_heating.infrastructure.measurements_bronze import MeteringPointType


def create_bronze_measurements(
    measurements_daily: DataFrame,
    orchestation_type: str,
    orchestration_instance_id: UUID,
    metering_point_type: MeteringPointType,
    resolution: Resolution = Resolution.HOURLY,
) -> DataFrame:
    transaction_creation_datetime = datetime.datetime.now()

    df = measurements_daily.withColumn("orchestration_type", F.lit(orchestation_type))
    df = df.withColumn("orchestration_instance_id", F.lit(orchestration_instance_id))
    df = df.withColumn("transaction_creation_datetime", F.lit(transaction_creation_datetime))
    df = df.withColumn("metering_point_type", F.lit(metering_point_type))
    df = df.withColumn("product", F.lit("8716867000030"))
    df = df.withColumn("resolution", F.lit(resolution.value))
    df = df.withColumn("unit", F.lit("kWh"))

    return df
