from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain.model import calculated_measurements_factory
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsRepository
from geh_calculated_measurements.electrical_heating.application.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from geh_calculated_measurements.electrical_heating.domain import (
    execute,
)
from geh_calculated_measurements.electrical_heating.infrastructure import (
    ElectricityMarketRepository,
    MeasurementsGoldRepository,
)


@use_span()
def execute_application(spark: SparkSession, args: ElectricalHeatingArgs) -> None:
    # Create repositories to obtain data frames
    electricity_market_repository = ElectricityMarketRepository(spark, args.electricity_market_data_path)
    measurements_gold_repository = MeasurementsGoldRepository(spark, args.catalog_name)

    # Read data frames
    time_series_points = measurements_gold_repository.read_time_series_points()
    consumption_metering_point_periods = electricity_market_repository.read_consumption_metering_point_periods()
    child_metering_point_periods = electricity_market_repository.read_child_metering_points()

    # Execute the domain logic
    calculated_measurements_daily = execute(
        time_series_points,
        consumption_metering_point_periods,
        child_metering_point_periods,
        args.time_zone,
    )

    # Write the calculated measurements
    calculated_measurements = calculated_measurements_factory.create(
        measurements=calculated_measurements_daily,
        orchestration_instance_id=args.orchestration_instance_id,
        orchestration_type=OrchestrationType.ELECTRICAL_HEATING,
        metering_point_type=MeteringPointType.ELECTRICAL_HEATING,
        time_zone=args.time_zone,
    )
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements)
