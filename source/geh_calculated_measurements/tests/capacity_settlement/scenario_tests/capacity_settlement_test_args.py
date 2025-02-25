import os
import uuid

from dotenv import load_dotenv

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import CapacitySettlementArgs


class CapacitySettlementTestArgs(CapacitySettlementArgs):
    """Args for testing the electrical heating job."""

    def __init__(self, env_file_path: str) -> None:
        load_dotenv(env_file_path, override=True)

        catalog_name = os.getenv("CATALOG_NAME", "DEFAULT_CATALOG_NAME")
        orchestration_instance_id = uuid.UUID(os.getenv("ORCHESTRATION_INSTANCE_ID", "DEFAULT_ORCHESTRATION_INSTANCE_ID"))
        calculation_month = int(os.getenv("CALCULATION_MONTH", "DEFAULT_CALCULATION_MONTH"))
        calculation_year = int(os.getenv("CALCULATION_YEAR", "DEFAULT_CALCULATION_YEAR"))
        
        super().__init__(
            catalog_name=catalog_name,
            orchestration_instance_id=orchestration_instance_id,
            calculation_month=calculation_month,
            calculation_year=calculation_year,
        )
