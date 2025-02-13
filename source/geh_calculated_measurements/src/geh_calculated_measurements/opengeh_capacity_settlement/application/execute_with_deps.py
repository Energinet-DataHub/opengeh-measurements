from geh_common.telemetry.decorators import use_span
from geh_common.telemetry.logger import Logger

from geh_calculated_measurements.opengeh_capacity_settlement.application.job_args.capacity_settlement_args import (
    CapacitySettlementArgs,
)


@use_span()
def execute_with_deps(job_arguments: CapacitySettlementArgs):
    logger = Logger(__name__)
    logger.info(f"Command line arguments: {job_arguments}")
