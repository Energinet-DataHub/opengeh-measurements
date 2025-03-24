from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)


@use_span()
def execute_application(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    pass
