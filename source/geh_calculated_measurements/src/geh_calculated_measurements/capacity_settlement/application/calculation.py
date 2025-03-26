from datetime import UTC, datetime
from uuid import UUID

from geh_common.domain.types import MeteringPointType, OrchestrationType
from geh_common.telemetry import use_span
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs
from geh_calculated_measurements.capacity_settlement.application.model.calculations import Calculations
from geh_calculated_measurements.capacity_settlement.domain.calculation import execute
from geh_calculated_measurements.capacity_settlement.domain.calculation_output import (
    CalculationOutput,
)
from geh_calculated_measurements.capacity_settlement.infrastructure import (
    CapacitySettlementRepository,
    ElectricityMarketRepository,
    MeasurementsGoldRepository,
)
from geh_calculated_measurements.common.domain import (
    ContractColumnNames,
)
from geh_calculated_measurements.common.domain.model import calculated_measurements_factory
from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsRepository, initialize_spark


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
        args.calculation_month,
        args.calculation_year,
        args.time_zone,
    )

    calculations = _create_calculations(
        args.orchestration_instance_id,
        args.calculation_month,
        args.calculation_year,
    )

    calculated_measurements_hourly = calculated_measurements_factory.create(
        calculation_output.calculated_measurements_daily,
        args.orchestration_instance_id,
        OrchestrationType.CAPACITY_SETTLEMENT,
        MeteringPointType.CAPACITY_SETTLEMENT,
        args.time_zone,
    )

    # Create a repository to write the calculated measurements
    calculated_measurements_repository = CalculatedMeasurementsRepository(spark, args.catalog_name)

    # Write the calculated measurements
    calculated_measurements_repository.write_calculated_measurements(calculated_measurements_hourly)

    # Create a repository to write the calculations and ten largest quantities
    capacity_settlement_repository = CapacitySettlementRepository(spark, args.catalog_name)

    # Write the calculations
    capacity_settlement_repository.write_calculations(calculations.df)

    # Write the ten largest quantities
    ten_largest_quantities = calculation_output.ten_largest_quantities.df.withColumn(
        ContractColumnNames.orchestration_instance_id, F.lit(str(args.orchestration_instance_id))
    )
    capacity_settlement_repository.write_ten_largest_quantities(ten_largest_quantities)


def _create_calculations(
    orchestration_instance_id: UUID,
    calculation_month: int,
    calculation_year: int,
) -> Calculations:
    execution_time = datetime.now(UTC).replace(microsecond=0)
    schema = T.StructType(
        [
            T.StructField("orchestration_instance_id", T.StringType(), False),
            T.StructField("calculation_year", T.IntegerType(), False),
            T.StructField("calculation_month", T.IntegerType(), False),
            T.StructField("execution_time", T.TimestampType(), False),
        ]
    )
    spark = initialize_spark()
    return Calculations(
        spark.createDataFrame(
            [
                (
                    str(orchestration_instance_id),
                    calculation_year,
                    calculation_month,
                    execution_time,
                )
            ],
            schema=schema,
        )
    )
