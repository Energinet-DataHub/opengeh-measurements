from geh_calculated_measurements.capacity_settlement.application import (
    CapacitySettlementArgs,
)


class CapacitySettlementTestArgs(CapacitySettlementArgs):
    """Args for testing the electrical heating job."""

    def __init__(self, env_file_path: str) -> None:
        super().__init__(_env_file=env_file_path)
