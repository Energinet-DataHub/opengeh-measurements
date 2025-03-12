from geh_common.telemetry.decorators import use_span
from pyspark.sql import SparkSession

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.capacity_settlement.domain.calculation_output import CalculationOutput
from geh_calculated_measurements.capacity_settlement.infrastructure import (
    CapacitySettlementRepository,
    ElectricityMarketRepository,
    MeasurementsGoldRepository,
)
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsRepository


@use_span()
def execute_application(spark: SparkSession, args: CapacitySettlementArgs) -> None:
    # Create repositories to obtain data frames
    measurements_gold_repository = MeasurementsGoldRepository(spark, args.catalog_name)
    electricity_market_repository = ElectricityMarketRepository(spark, args.electricity_market_data_path)

    # Read data frames
    time_series_points = measurements_gold_repository.read_time_series_points()
    metering_point_periods = electricity_market_repository.read_metering_point_periods()

    # Execute the domain logic
    calculation_output: CalculationOutput = execute(
        time_series_points,
        metering_point_periods,
        args.orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )

    # Create a repository to write the calculated measurements
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)

    # Write the calculated measurements
    calculated_measurements = calculation_output.calculated_measurements
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements)

    # Create a repository to write the calculations and ten largest quantities
    capacity_settlement_repository = CapacitySettlementRepository(spark, args.catalog_name)

    # Write the calculations
    calculations = calculation_output.calculations
    capacity_settlement_repository.write_calculations(calculations)

    # Write the ten largest quantities
    ten_largest_quantities = calculation_output.ten_largest_quantities
    capacity_settlement_repository.write_ten_largest_quantities(ten_largest_quantities)
