"""Entry point for the capacity-settlement application."""

from geh_common.telemetry.decorators import start_trace
from geh_common.telemetry.logger import Logger
from geh_common.telemetry.logging_configuration import LoggingSettings, configure_logging

from geh_calculated_measurements.opengeh_capacity_settlement.application.execute_with_deps import (
    execute_with_deps,
)
from geh_calculated_measurements.opengeh_capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)


def execute() -> None:
    electrical_heating_args = CapacitySettlementArgs()  # Retrieve calculation oriented settings / job arguments
    logging_settings = LoggingSettings(subsystem="measurements", cloud_role_name="dbr-capacity-settlement")
    configure_logging(logging_settings=logging_settings)
    orchestrate_business_logic(job_arguments=electrical_heating_args, logging_settings=logging_settings)


@start_trace(
    initial_span_name="capacity-settlement-job"
)  # Wraps orchestrate_business_logic with start_trace: starting a tracer, and provides an initial span with name initial_span_name
def orchestrate_business_logic(job_arguments: CapacitySettlementArgs, logging_settings: LoggingSettings) -> None:
    logger = Logger(__name__)
    logger.info(f"Command line arguments / env variables retrieved for Logging Settings: {logging_settings}")
    logger.info(f"Command line arguments retrieved for electrical heating job Oriented Parameters: {job_arguments}")
    execute_with_deps(job_arguments=job_arguments)
