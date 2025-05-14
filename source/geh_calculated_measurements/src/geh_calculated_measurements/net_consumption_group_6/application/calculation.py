import pyspark.sql.functions as F
from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.telemetry.decorators import use_span
from pyspark.sql import DataFrame, SparkSession, Window

from geh_calculated_measurements.common.application.model import (
    calculated_measurements_hourly_factory,
)
from geh_calculated_measurements.common.domain.column_names import ContractColumnNames
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


def get_current_with_settlement_type(spark: SparkSession, args: NetConsumptionGroup6Args) -> DataFrame:
    """Get current measurements with settlement type from the calculated measurements.

    This function reads the current measurements and calculated measurements from the repository,
    filters the calculated measurements to get the latest one for each metering point and observation time,
    and joins it with the current measurements to get the settlement type.
    """
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)
    calculated_measurments_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)

    current_measurements = current_measurements_repository.read_current_measurements().df
    calculated_measurments = calculated_measurments_repository.read_calculated_measurements().df

    current_measurements = current_measurements.alias("ts")
    calculated_measurments = calculated_measurments.alias("cmi")

    calculated_measurements_ranked = calculated_measurments.withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy(
                calculated_measurments[ContractColumnNames.metering_point_id],
                calculated_measurments[ContractColumnNames.observation_time],
            ).orderBy(F.desc(calculated_measurments.transaction_creation_datetime))
        ),
    )

    calculated_measurements_filtered = calculated_measurements_ranked.filter(F.col("row_number") == 1).drop(
        "row_number"
    )

    current_measurements_with_settlement_type = current_measurements.join(
        calculated_measurements_filtered,
        on=[
            current_measurements[ContractColumnNames.metering_point_id]
            == calculated_measurements_filtered[ContractColumnNames.metering_point_id],
            current_measurements[ContractColumnNames.observation_time]
            == calculated_measurements_filtered[ContractColumnNames.observation_time],
        ],
        how="left",
    ).select(
        "ts.*",
        calculated_measurements_filtered[ContractColumnNames.settlement_type].alias(
            ContractColumnNames.settlement_type
        ),
    )

    return current_measurements_with_settlement_type


@use_span()
def execute_application_cenc_daily(spark: SparkSession, args: NetConsumptionGroup6Args) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.catalog_name)
    current_measurements_repository = CurrentMeasurementsRepository(spark, args.catalog_name)

    # Read data frames
    current_measurements = current_measurements_repository.read_current_measurements()
    consumption_metering_point_periods = (
        electricity_market_repository.read_net_consumption_group_6_consumption_metering_point_periods()
    )
    child_metering_points = electricity_market_repository.read_net_consumption_group_6_child_metering_points()

    _, calculated_measurements_daily = execute_cenc_daily(
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
