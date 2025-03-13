from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.net_consumption_group_six.application import NetConsumptionGroupSixArgs
from geh_calculated_measurements.net_consumption_group_six.domain import execute


@use_span()
def execute_application(spark: SparkSession, args: NetConsumptionGroupSixArgs) -> None:
    execute()
