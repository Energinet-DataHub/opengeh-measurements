from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)


@use_span()
def execute_application(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    dummy()
    dummy2()


@use_span()
def dummy() -> int:
    return 1 + 1


@use_span()
def dummy2() -> int:
    return 1 + 1
