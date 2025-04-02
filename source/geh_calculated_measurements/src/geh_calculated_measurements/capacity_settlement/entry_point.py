"""Entry point for the capacity-settlement application."""

from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging

from geh_calculated_measurements.capacity_settlement.application import execute_application
from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import (
    CapacitySettlementArgs,
)
from geh_calculated_measurements.common.infrastructure.spark_initializor import initialize_spark


def execute() -> None:
    capacity_settlement_args = CapacitySettlementArgs()  # Retrieve calculation oriented settings / job arguments
    logging_settings = configure_logging(subsystem="measurements", cloud_role_name="dbr-capacity-settlement")
    orchestrate_business_logic(job_arguments=capacity_settlement_args, logging_settings=logging_settings)


@start_trace()
def orchestrate_business_logic(job_arguments: CapacitySettlementArgs, logging_settings: LoggingSettings) -> None:
    logger = Logger(__name__)
    logger.info(f"Command line arguments / env variables retrieved for Logging Settings: {logging_settings}")
    logger.info(f"Command line arguments retrieved for capacity settlement job Oriented Parameters: {job_arguments}")
    spark = initialize_spark()
    execute_application(spark, args=job_arguments)
