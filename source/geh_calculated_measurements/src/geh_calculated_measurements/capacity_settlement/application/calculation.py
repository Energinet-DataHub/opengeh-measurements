from geh_common.telemetry.decorators import use_span
from geh_common.telemetry.logger import Logger

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import (
    CapacitySettlementArgs,
)


@use_span()
def execute(args: CapacitySettlementArgs):
    logger = Logger(__name__)
    logger.info(f"Application arguments: {args}")
