from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from geh_calculated_measurements.common.application.model import (
    CalculatedMeasurementsInternal,
    calculated_measurements_hourly_factory,
)
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


def get_current_calculated_measurements(
    calculated_measurements: CalculatedMeasurementsInternal, metering_point_type: MeteringPointType
) -> CalculatedMeasurementsInternal:
    calculated_measurements_df = calculated_measurements.df.filter(
        calculated_measurements.df.metering_point_type == metering_point_type.value
    )
    # Define a window specification to retrieve the most recent record for each key combination
    window_spec = Window.partitionBy("metering_point_id", "observation_time").orderBy(
        F.desc("transaction_creation_datetime")
    )

    # Add row number and filter to keep only the newest record
    df_with_row_num = calculated_measurements_df.withColumn("row_num", F.row_number().over(window_spec))
    filtered_df = df_with_row_num.filter("row_num = 1").drop("row_num")

    return CalculatedMeasurementsInternal(filtered_df)


@use_span()
def execute_application_cenc_daily(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.catalog_name)
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    current_measurements = current_measurements_repository.read_current_measurements()
    consumption_metering_point_periods = (
        electricity_market_repository.read_net_consumption_group_6_consumption_metering_point_periods()
    )
    child_metering_points = electricity_market_repository.read_net_consumption_group_6_child_metering_points()
    calculated_measurements = calculated_measurements_repository.read_calculated_measurements()
    current_calculated_measurements = get_current_calculated_measurements(
        calculated_measurements, MeteringPointType.NET_CONSUMPTION
    )

    _, calculated_measurements_daily = execute_cenc_daily(
        current_measurements=current_measurements,
        calculated_measurements=current_calculated_measurements,
        consumption_metering_point_periods=consumption_metering_point_periods,
        child_metering_points=child_metering_points,
        time_zone=args.time_zone,
        execution_start_datetime=args.execution_start_datetime,
    )

    # Write the calculated measurements
    calculated_measurements_hourly = calculated_measurements_hourly_factory.create(
        calculated_measurements_daily,
        args.orchestration_instance_id,
        OrchestrationType.NET_CONSUMPTION,
        MeteringPointType.NET_CONSUMPTION,
        args.time_zone,
        transaction_creation_datetime=args.execution_start_datetime,
    )

    calculated_measurements_repository.write_calculated_measurements(calculated_measurements_hourly)


@use_span()
def execute_application_cnc_daily(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.catalog_name)
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    current_measurements = current_measurements_repository.read_current_measurements()
    consumption_metering_point_periods = (
        electricity_market_repository.read_net_consumption_group_6_consumption_metering_point_periods()
    )
    child_metering_points = electricity_market_repository.read_net_consumption_group_6_child_metering_points()

    calculated_measurements_daily = execute_cnc_daily(
        current_measurements=current_measurements,
        consumption_metering_point_periods=consumption_metering_point_periods,
        child_metering_points=child_metering_points,
        time_zone=args.time_zone,
        execution_start_datetime=args.execution_start_datetime,
    )

    # Write the calculated measurements
    calculated_measurements_hourly = calculated_measurements_hourly_factory.create(
        calculated_measurements_daily,
        args.orchestration_instance_id,
        OrchestrationType.NET_CONSUMPTION,
        MeteringPointType.NET_CONSUMPTION,
        args.time_zone,
        transaction_creation_datetime=args.execution_start_datetime,
    )
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements_hourly)
