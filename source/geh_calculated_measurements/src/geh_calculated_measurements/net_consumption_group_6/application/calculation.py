from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.application.model import calculated_measurements_factory
from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsRepository,
    CurrentMeasurementsRepository,
)
from geh_calculated_measurements.net_consumption_group_6.application.net_consumption_group_6_args import (
    NetConsumptionGroup6Args,
)
from geh_calculated_measurements.net_consumption_group_6.domain.calculations import (
    execute_cenc_daily,
    execute_cnc_daily,
)
from geh_calculated_measurements.net_consumption_group_6.infrastucture import (
    ElectricityMarketRepository,
)


@use_span()
def execute_application_cenc_daily(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(
        spark, args.catalog_name, args.electricity_market_database_name
    )
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    current_measurements = current_measurements_repository.read_current_measurements()
    consumption_metering_point_periods = (
        electricity_market_repository.read_net_consumption_group_6_consumption_metering_point_periods()
    )
    child_metering_points = electricity_market_repository.read_net_consumption_group_6_child_metering_points()

    _, calculated_measurements_daily = execute_cenc_daily(
        current_measurements,
        consumption_metering_point_periods,
        child_metering_points,
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
        transaction_creation_datetime=args.execution_start_datetime,
    )
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements_hourly)


@use_span()
def execute_application_cnc_daily(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(
        spark, args.catalog_name, args.electricity_market_database_name
    )
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    current_measurements = current_measurements_repository.read_current_measurements()
    consumption_metering_point_periods = (
        electricity_market_repository.read_net_consumption_group_6_consumption_metering_point_periods()
    )
    child_metering_points = electricity_market_repository.read_net_consumption_group_6_child_metering_points()

    _, calculated_measurements_daily = execute_cnc_daily(
        current_measurements,
        consumption_metering_point_periods,
        child_metering_points,
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
        transaction_creation_datetime=args.execution_start_datetime,
    )
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements_hourly)
