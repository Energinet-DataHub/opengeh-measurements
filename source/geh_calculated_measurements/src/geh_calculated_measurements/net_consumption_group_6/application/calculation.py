from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain.model import calculated_measurements_factory
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsRepository
from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)
from geh_calculated_measurements.net_consumption_group_6.domain import execute


@use_span()
def execute_application(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Read data frames
    time_series_points = None
    consumption_metering_point_periods = None
    child_metering_point_periods = None

    _, calculated_measurements_daily = execute(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
        args.execution_start_datetime,
    )

    # Write the calculated measurements
    calculated_measurements_hourly = calculated_measurements_factory.create(
        calculated_measurements_daily,
        args.orchestration_instance_id,
        OrchestrationType.NET_CONSUMPTION,
        MeteringPointType.NET_CONSUMPTION,
        args.time_zone,
    )
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements_hourly)
