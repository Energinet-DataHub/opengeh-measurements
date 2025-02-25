from geh_calculated_measurements.capacity_settlement.application import (
    CapacitySettlementArgs,
)


class CapacitySettlementTestArgs:
    """Args for testing the electrical heating job."""

    def __init__(self, env_file_path: str) -> None:
        self.args = CapacitySettlementArgs(_env_file=env_file_path)
